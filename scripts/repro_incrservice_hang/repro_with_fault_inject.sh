#!/bin/bash
# ============================================================================
# 复现 incrservice asyncAllocate 卡死 Bug — 故障注入方式
#
# 完整复现生产 bug 路径：
#   1. lockMoTable 返回 ErrTxnNeedRetryWithDefChanged（模拟并发 DDL 冲突）
#   2. Compile.Run retry 循环触发 prepareRetry(defChanged=true)，重建 plan
#   3. 重试时 lockMoTable 正常通过，dbSource.Create 成功
#   4. maybeCreateAutoIncrement → incrservice.Create → preAllocate 卡住
#   5. txn 泄漏，锁永远不释放
#
# 使用两个故障注入点：
#   - lock_mo_table_def_changed: 在 lockMoTable 中注入，freq "1:1::" 只触发一次
#   - incrservice_allocate_hang: 在 preAllocate 中注入 sleep，模拟 TN 卡住
#
# 兼容：单机版（mo-service standalone）和分布式集群均可使用。
#
# 用法：
#   bash repro_with_fault_inject.sh [MO_HOST] [MO_PORT] [MO_USER] [MO_PASSWORD]
# ============================================================================

set -euo pipefail

MO_HOST="${1:-127.0.0.1}"
MO_PORT="${2:-6001}"
MO_USER="${3:-root}"
MO_PASS="${4:-111}"
DB_NAME="repro_incr_hang_fi"
HANG_SECONDS=120  # 模拟卡住 2 分钟

MYSQL="mysql -h${MO_HOST} -P${MO_PORT} -u${MO_USER} -p${MO_PASS} --connect-timeout=5"

echo "=== incrservice hang 完整复现（故障注入方式）==="
echo "MO: ${MO_HOST}:${MO_PORT}"
echo "模拟卡住时间: ${HANG_SECONDS}s"
echo ""
echo "复现路径："
echo "  lockMoTable → ErrTxnNeedRetryWithDefChanged → retry → incrservice hang"
echo ""

# 清理
cleanup() {
    echo ""
    echo "[cleanup] 移除故障点..."
    ${MYSQL} -e "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'lock_mo_table_def_changed');" 2>/dev/null || true
    ${MYSQL} -e "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'incrservice_allocate_hang');" 2>/dev/null || true
    ${MYSQL} -e "SELECT disable_fault_injection();" 2>/dev/null || true
    echo "[cleanup] 清理测试数据库..."
    ${MYSQL} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
    jobs -p | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# Step 1: 创建测试数据库
echo "[1/7] 创建测试数据库..."
${MYSQL} -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME};" 2>/dev/null

# Step 2: 启用 fault injection 框架（默认关闭）
echo "[2/7] 启用 fault injection 框架..."
RESULT=$(${MYSQL} -N -e "SELECT enable_fault_injection();" 2>/dev/null)
echo "  结果: ${RESULT}"

# Step 3: 注入故障点 1 — lockMoTable 返回 ErrTxnNeedRetryWithDefChanged
# freq "1:1::" = 只在第 1 次调用时触发，之后不再触发（retry 时 lockMoTable 正常通过）
# action=echo: TriggerFault 返回 ok=true，不阻塞
echo "[3/7] 注入故障点 1: lock_mo_table_def_changed (触发一次 retry)..."
RESULT=$(${MYSQL} -N -e "SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lock_mo_table_def_changed#1:1:::#echo#0##false');" 2>/dev/null)
echo "  结果: ${RESULT}"

# Step 4: 注入故障点 2 — incrservice preAllocate 卡住
# freq ":::" = 每次都触发
# action=sleep: 阻塞 HANG_SECONDS 秒
echo "[4/7] 注入故障点 2: incrservice_allocate_hang (sleep ${HANG_SECONDS}s)..."
RESULT=$(${MYSQL} -N -e "SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_allocate_hang#:::#sleep#${HANG_SECONDS}##false');" 2>/dev/null)
echo "  结果: ${RESULT}"

# Step 5: 在后台执行 CREATE TABLE（会卡住）
echo "[5/7] 在后台执行 CREATE TABLE..."
echo "  预期流程："
echo "    1. lockMoTable → ErrTxnNeedRetryWithDefChanged（故障注入）"
echo "    2. Compile.Run retry → prepareRetry(defChanged=true)"
echo "    3. 重试: lockMoTable 正常 → dbSource.Create 成功"
echo "    4. maybeCreateAutoIncrement → incrservice.Create → preAllocate 卡住 ${HANG_SECONDS}s"
echo ""
START_TIME=$(date +%s)
${MYSQL} --connect-timeout=300 -e "
    CREATE TABLE ${DB_NAME}.staff_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        age INT
    );
" 2>&1 &
DDL_PID=$!
echo "  DDL PID: ${DDL_PID}"

# Step 6: 等待几秒，然后检查 DDL 是否还在运行
echo "[6/7] 等待 10s 后检查 DDL 状态..."
sleep 10

if kill -0 ${DDL_PID} 2>/dev/null; then
    ELAPSED=$(($(date +%s) - START_TIME))
    echo ""
    echo "  ✅ 复现成功！CREATE TABLE 已卡住 ${ELAPSED}s"
    echo "  DDL 进程 (PID ${DDL_PID}) 仍在运行"
    echo ""
    echo "  此时在 MO 日志中应该能看到："
    echo "    - 'FAULT INJECTION: incrservice_allocate_hang triggered'"
    echo "    - 每 20s: 'found leak txn'"
    echo "    - Compile.Run 的 retry 日志"
    echo ""

    # Step 7: 等待 DDL 完成或超时
    echo "[7/7] 等待 DDL 完成（sleep 结束后会自动恢复）..."
    echo "  或者按 Ctrl+C 取消..."
    wait ${DDL_PID} 2>/dev/null
    ELAPSED=$(($(date +%s) - START_TIME))
    echo "  DDL 完成，总耗时: ${ELAPSED}s"
else
    ELAPSED=$(($(date +%s) - START_TIME))
    echo ""
    echo "  ❌ DDL 已完成（耗时 ${ELAPSED}s），故障注入可能未生效"
    echo "  请检查："
    echo "    1. MO 是否包含 ddl.go 和 column_cache.go 的故障注入代码"
    echo "    2. 表是否有 AUTO_INCREMENT 列"
    echo "    3. enable_fault_injection() 是否返回 true"
    echo "    4. MO 日志中是否有 'FAULT INJECTION' 或 'TriggerFault' 关键字"
fi

echo ""
echo "=== 完成 ==="

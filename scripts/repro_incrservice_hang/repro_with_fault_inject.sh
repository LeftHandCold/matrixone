#!/bin/bash
# ============================================================================
# 复现 incrservice asyncAllocate 卡死 Bug — 故障注入方式
#
# 前提：MO 已编译并包含 column_cache.go 中的 fault injection 代码
# 原理：通过 MO 内置的 fault injection 框架，在 preAllocate 中注入 sleep，
#       模拟 asyncAllocate RPC 到 TN 卡住的场景。
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

echo "=== incrservice hang 复现（故障注入方式）==="
echo "MO: ${MO_HOST}:${MO_PORT}"
echo "模拟卡住时间: ${HANG_SECONDS}s"
echo ""

# 清理
cleanup() {
    echo ""
    echo "[cleanup] 移除故障点..."
    ${MYSQL} -e "SELECT mo_ctl('cn', 'RemoveFaultPoint', 'incrservice_allocate_hang');" 2>/dev/null || true
    echo "[cleanup] 清理测试数据库..."
    ${MYSQL} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
    jobs -p | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# Step 1: 创建测试数据库
echo "[1/5] 创建测试数据库..."
${MYSQL} -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME};" 2>/dev/null

# Step 2: 启用故障注入
echo "[2/5] 启用故障注入: incrservice_allocate_hang (sleep ${HANG_SECONDS}s)..."
# fault.AddFaultPoint 的 sleep action 会让 TriggerFault 阻塞 iarg 秒
# freq ":::" 表示每次都触发
RESULT=$(${MYSQL} -N -e "SELECT mo_ctl('cn', 'AddFaultPoint', 'incrservice_allocate_hang.:::.sleep.${HANG_SECONDS}.');" 2>/dev/null)
echo "  故障注入结果: ${RESULT}"

# Step 3: 在后台执行 CREATE TABLE（会卡住）
echo "[3/5] 在后台执行 CREATE TABLE（预期会卡住 ${HANG_SECONDS}s）..."
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

# Step 4: 等待几秒，然后检查 DDL 是否还在运行
echo "[4/5] 等待 10s 后检查 DDL 状态..."
sleep 10

if kill -0 ${DDL_PID} 2>/dev/null; then
    ELAPSED=$(($(date +%s) - START_TIME))
    echo ""
    echo "  ✅ 复现成功！CREATE TABLE 已卡住 ${ELAPSED}s"
    echo "  DDL 进程 (PID ${DDL_PID}) 仍在运行"
    echo ""
    echo "  此时在 MO 日志中应该能看到："
    echo "    - FAULT INJECTION: incrservice_allocate_hang triggered"
    echo "    - 每 20s 一次: ERROR cn-service found leak txn"
    echo ""
    echo "  在另一个 session 中执行以下 SQL 可以看到卡住的 txn："
    echo "    SELECT * FROM system.statement_info WHERE status = 'Running' ORDER BY request_at DESC LIMIT 10;"
    echo ""

    # Step 5: 等待 DDL 完成或超时
    echo "[5/5] 等待 DDL 完成（故障注入 sleep 结束后会自动恢复）..."
    echo "  或者按 Ctrl+C 取消..."
    wait ${DDL_PID} 2>/dev/null
    ELAPSED=$(($(date +%s) - START_TIME))
    echo "  DDL 完成，总耗时: ${ELAPSED}s"
else
    ELAPSED=$(($(date +%s) - START_TIME))
    echo ""
    echo "  ❌ DDL 已完成（耗时 ${ELAPSED}s），故障注入可能未生效"
    echo "  请检查："
    echo "    1. MO 是否包含 column_cache.go 的故障注入代码"
    echo "    2. 表是否有 AUTO_INCREMENT 列"
    echo "    3. MO 日志中是否有 'FAULT INJECTION' 关键字"
fi

echo ""
echo "=== 完成 ==="

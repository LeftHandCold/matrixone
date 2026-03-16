#!/bin/bash
# ============================================================================
# 复现 incrservice asyncAllocate 卡死 Bug — 并发 DDL 方式
#
# 原理：多个 session 并发对同一个表名执行 CREATE TABLE，触发 lock 冲突，
# 使得其中一个 session 遇到 ErrTxnNeedRetryWithDefChanged 并重试。
# 重试时 maybeCreateAutoIncrement 调用 incrservice.Create()，如果此时
# TN 响应慢，asyncAllocate 会卡住。
#
# 注意：这个脚本不保证 100% 复现 TN 卡死，但可以稳定触发 retry 路径。
# 要完整复现需要配合故障注入（见 README.md 方式 2）。
#
# 用法：
#   bash repro_concurrent_ddl.sh [MO_HOST] [MO_PORT] [MO_USER] [MO_PASSWORD]
# ============================================================================

set -euo pipefail

MO_HOST="${1:-127.0.0.1}"
MO_PORT="${2:-6001}"
MO_USER="${3:-root}"
MO_PASS="${4:-111}"
DB_NAME="repro_incr_hang_$(date +%s)"
TABLE_NAME="staff_info"
CONCURRENCY=10
ROUNDS=50

MYSQL_CMD="mysql -h${MO_HOST} -P${MO_PORT} -u${MO_USER} -p${MO_PASS} --connect-timeout=5"

echo "=== incrservice hang 复现脚本 ==="
echo "MO: ${MO_HOST}:${MO_PORT}"
echo "DB: ${DB_NAME}"
echo "并发数: ${CONCURRENCY}"
echo "轮次: ${ROUNDS}"
echo ""

# 创建测试数据库
echo "[1/4] 创建测试数据库..."
${MYSQL_CMD} -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};" 2>/dev/null

# 清理函数
cleanup() {
    echo ""
    echo "[cleanup] 清理测试数据库..."
    ${MYSQL_CMD} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
    # 杀掉所有后台进程
    jobs -p | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# 单个 worker：循环 DROP + CREATE TABLE
worker() {
    local id=$1
    local success=0
    local retry_err=0
    local other_err=0

    for ((r=1; r<=ROUNDS; r++)); do
        # DROP IF EXISTS（忽略错误）
        ${MYSQL_CMD} -e "DROP TABLE IF EXISTS ${DB_NAME}.${TABLE_NAME};" 2>/dev/null || true

        # CREATE TABLE（可能触发 ErrTxnNeedRetryWithDefChanged）
        output=$(${MYSQL_CMD} -e "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            dept VARCHAR(50)
        );" 2>&1) || true

        if echo "$output" | grep -qi "already exists"; then
            # 另一个 worker 先创建了，正常
            :
        elif echo "$output" | grep -qi "retry"; then
            retry_err=$((retry_err + 1))
        elif echo "$output" | grep -qi "error\|ERROR"; then
            other_err=$((other_err + 1))
        else
            success=$((success + 1))
        fi
    done

    echo "  worker-${id}: success=${success} retry=${retry_err} other_err=${other_err}"
}

echo "[2/4] 启动 ${CONCURRENCY} 个并发 worker，每个执行 ${ROUNDS} 轮 DROP+CREATE..."
echo ""

pids=()
for ((i=1; i<=CONCURRENCY; i++)); do
    worker $i &
    pids+=($!)
done

echo "[3/4] 等待所有 worker 完成..."
for pid in "${pids[@]}"; do
    wait $pid 2>/dev/null || true
done

echo ""
echo "[4/4] 检查是否有残留的 leak txn..."
echo "请检查 MO 日志中是否有以下关键字："
echo '  grep "found leak txn" /path/to/mo-cn.log'
echo '  grep "ErrTxnNeedRetryWithDefChanged" /path/to/mo-cn.log'
echo '  grep "maybeCreateAutoIncrement" /path/to/mo-cn.log'
echo ""
echo "如果看到 'found leak txn' 持续出现且不消失，说明复现了 bug。"
echo ""
echo "=== 完成 ==="

#!/bin/bash
# 复现 lockWithRetry 无限循环 Bug
#
# 前置条件：
#   1. 已编译包含 lockservice_handle_remote_lock_hang fault point 的二进制
#   2. 多 CN 集群已启动（CN1: 16001, CN2: 16002）
#
# 用法：
#   bash doc/reproduce_lockwithretry_infinite_loop.sh

set -euo pipefail

CN1_PORT=16001
CN2_PORT=16002
MYSQL_USER=root
MYSQL_PASS=111
DB_NAME=test_retry_loop

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

mysql_cn1() {
    mysql -h 127.0.0.1 -P "$CN1_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names -e "$1" 2>/dev/null
}

mysql_cn2() {
    mysql -h 127.0.0.1 -P "$CN2_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names -e "$1" 2>/dev/null
}

cleanup() {
    echo -e "${YELLOW}清理中...${NC}"
    mysql_cn1 "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'lockservice_handle_remote_lock_hang');" 2>/dev/null || true
    mysql_cn1 "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
    # 杀掉后台 CREATE TABLE 进程
    if [[ -n "${CREATE_PID:-}" ]]; then
        kill "$CREATE_PID" 2>/dev/null || true
        wait "$CREATE_PID" 2>/dev/null || true
    fi
    if [[ -n "${LOG_PID:-}" ]]; then
        kill "$LOG_PID" 2>/dev/null || true
        wait "$LOG_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "============================================"
echo "  lockWithRetry 无限循环 Bug 复现"
echo "============================================"
echo ""

# Step 1: 清理旧数据
echo -e "${YELLOW}[Step 1] 清理旧数据...${NC}"
mysql_cn1 "DROP DATABASE IF EXISTS $DB_NAME;" || true

# Step 2: CN2 创建 seed 表
echo -e "${YELLOW}[Step 2] CN2 创建 seed 表（让锁表绑定到 CN2）...${NC}"
mysql_cn2 "CREATE DATABASE IF NOT EXISTS $DB_NAME;"
mysql_cn2 "USE $DB_NAME; CREATE TABLE seed_t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT);"
mysql_cn2 "USE $DB_NAME; INSERT INTO seed_t1 (v) VALUES (1);"
echo -e "${GREEN}  ✅ seed 表已创建，mo_increment_columns 锁表应绑定到 CN2${NC}"

# Step 3: 注入 fault point
echo -e "${YELLOW}[Step 3] 注入 fault point...${NC}"
mysql_cn1 "SELECT enable_fault_injection();"
mysql_cn1 "SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lockservice_handle_remote_lock_hang#:::#sleep#30##false');"
echo -e "${GREEN}  ✅ lockservice_handle_remote_lock_hang 已注入（CN2 处理 lock 请求时 sleep 30s）${NC}"

# Step 4: CN1 后台执行 CREATE TABLE
echo -e "${YELLOW}[Step 4] CN1 执行 CREATE TABLE（后台）...${NC}"
mysql -h 127.0.0.1 -P "$CN1_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
    -e "USE $DB_NAME; CREATE TABLE t_cn1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, val INT);" \
    2>/dev/null &
CREATE_PID=$!
echo -e "  CREATE TABLE PID: $CREATE_PID"

# Step 5: 监控日志
echo -e "${YELLOW}[Step 5] 等待 30 秒观察 CN1 日志...${NC}"
echo ""

LOCK_FAIL_COUNT=0
WAIT_SECONDS=90
INTERVAL=10

for ((i=1; i<=WAIT_SECONDS/INTERVAL; i++)); do
    sleep "$INTERVAL"

    # 检查 CREATE TABLE 是否还在运行
    if ! kill -0 "$CREATE_PID" 2>/dev/null; then
        echo -e "${RED}  ❌ CREATE TABLE 已退出（不应该退出）${NC}"
        wait "$CREATE_PID" 2>/dev/null
        echo -e "${RED}  Bug 未复现 — CREATE TABLE 没有卡住${NC}"
        exit 1
    fi

    # 检查 CN1 日志中的 lockWithRetry 诊断日志
    NEW_COUNT=$(docker logs mo-cn1 2>&1 | grep -c "lockWithRetry" || true)
    # 检查是否有 ctx.Err=context deadline exceeded（铁证）
    CTX_EXPIRED=$(docker logs mo-cn1 2>&1 | grep "lockWithRetry" | grep -c "ctx.Err=context deadline exceeded" || true)
    echo -e "  [${i}/${WAIT_SECONDS/INTERVAL}] CREATE TABLE 仍在运行，lockWithRetry 重试次数: $NEW_COUNT，ctx 已过期: $CTX_EXPIRED"
    LOCK_FAIL_COUNT=$NEW_COUNT
done

echo ""

# Step 6: 判定结果
if kill -0 "$CREATE_PID" 2>/dev/null && [[ "$LOCK_FAIL_COUNT" -gt 3 ]]; then
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}  🐛 lockWithRetry 无限循环 Bug 已复现！${NC}"
    echo -e "${RED}============================================${NC}"
    echo ""
    echo -e "  CN1 的 CREATE TABLE 已阻塞 ${WAIT_SECONDS} 秒"
    echo -e "  lockWithRetry 重试了 ${LOCK_FAIL_COUNT} 次"
    echo -e "  其中 ctx 已过期的次数: ${CTX_EXPIRED}"
    echo ""
    echo -e "  铁证：ctx.Err=context deadline exceeded 但循环仍在继续"
    echo -e "  原因：handleError 把 context.DeadlineExceeded 转成 ErrBackendCannotConnect"
    echo -e "        canRetryLock 看到 ErrBackendCannotConnect → 继续重试"
    echo ""
    echo -e "  ${YELLOW}修复方式：lockWithRetry 或 canRetryLock 检查 ctx.Err()${NC}"
    echo ""
    echo -e "  最后几条诊断日志："
    docker logs mo-cn1 2>&1 | grep "lockWithRetry" | tail -5
    exit 0
else
    echo -e "${YELLOW}  ⚠️ 结果不确定${NC}"
    echo -e "  CREATE TABLE running: $(kill -0 "$CREATE_PID" 2>/dev/null && echo yes || echo no)"
    echo -e "  Lock fail count: $LOCK_FAIL_COUNT"
    exit 1
fi

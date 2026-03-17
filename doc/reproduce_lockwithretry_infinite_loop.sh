#!/bin/bash
# 复现 lockWithRetry 无限循环 Bug
#
# 前置条件：
#   1. 已编译包含 remote_lock_short_timeout fault point 的二进制
#   2. 多 CN 集群已启动（CN1: 16001, CN2: 16002）
#
# 原理：
#   旧方案（handleRemoteLock sleep）不工作，因为 morpc 的 ping/pong 心跳
#   （每 2s）让连接保持活跃，readTimeout 永远不会触发。
#
#   新方案：在 remoteLockTable.lock() 中注入 fault point，将 client.Send
#   的 ctx 替换为 1s 超时的 ctx。这样每次 Send 都会在 1s 后超时，
#   handleError 把 DeadlineExceeded 转成 BackendCannotConnect，
#   canRetryLock 看到 BackendCannotConnect → 继续重试 → 无限循环。
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
    mysql_cn1 "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'remote_lock_short_timeout');" 2>/dev/null || true
    mysql_cn1 "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
    if [[ -n "${CREATE_PID:-}" ]]; then
        kill "$CREATE_PID" 2>/dev/null || true
        wait "$CREATE_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "============================================"
echo "  lockWithRetry 无限循环 Bug 复现"
echo "  (新方案: remote_lock_short_timeout)"
echo "============================================"
echo ""

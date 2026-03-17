#!/bin/bash
# =============================================================================
# 复现 isRetryError Bug：lockservice 孤儿检测失效
#
# 生产环境现象：泄漏事务持锁 7 小时，数百个事务被阻塞
# 根因：isRetryError 把 context deadline exceeded 当作可重试错误
#
# 使用方法：
#   bash doc/reproduce_isretryerror_bug.sh
#
# 前置条件：
#   1. 多 CN 集群已启动（docker compose --profile matrixone up -d）
#   2. CN1 端口 16001，CN2 端口 16002
#   3. 已编译包含 fault point 的 binary
#
# 复现原理：
#   1. CN2 先执行 CREATE TABLE，让 mo_increment_columns 的锁表绑定到 CN2
#   2. CN1 执行 CREATE TABLE，store.Allocate 获取 lockservice 行锁后 sleep（fault point）
#      → CN1 持有锁，allocator.run() 被阻塞
#   3. 注入 lockservice_get_active_txn_hang，让 CN1 的 GetActiveTxn RPC handler sleep
#   4. CN2 执行 CREATE TABLE → 需要同一行锁 → 等待 → orphan detection
#      → Method_GetActiveTxn RPC 到 CN1 → 超时 → isRetryError bug → 永远等待
# =============================================================================

set -euo pipefail

CN1_PORT=16001
CN2_PORT=16002
CN1_HOST=127.0.0.1
CN2_HOST=127.0.0.1
MYSQL_USER=root
MYSQL_PASS=111
DB_NAME=test_lock_leak
CN1_CONTAINER=mo-cn1
CN2_CONTAINER=mo-cn2

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "\n${CYAN}=== $* ===${NC}"; }

mysql_cn1() {
    mysql -h "$CN1_HOST" -P "$CN1_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names -e "$1" 2>/dev/null
}

mysql_cn2() {
    mysql -h "$CN2_HOST" -P "$CN2_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names -e "$1" 2>/dev/null
}

LEAK_TXN_PID=""
VICTIM_PID=""
LOG_MONITOR_PID=""

cleanup() {
    log_step "清理环境"

    # 清除 fault points
    mysql_cn1 "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'lockservice_get_active_txn_hang');" 2>/dev/null || true
    mysql_cn1 "SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'incrservice_after_for_update');" 2>/dev/null || true

    # 杀掉后台进程
    for pid_var in LEAK_TXN_PID VICTIM_PID LOG_MONITOR_PID; do
        pid="${!pid_var:-}"
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done

    # 清理数据库
    mysql_cn1 "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true

    log_info "清理完成"
}

trap cleanup EXIT

# =============================================================================
log_step "Step 1: 检查集群状态"
# =============================================================================

if ! mysql_cn1 "SELECT 1;" > /dev/null 2>&1; then
    log_error "CN1 (port $CN1_PORT) 不可达"
    exit 1
fi
if ! mysql_cn2 "SELECT 1;" > /dev/null 2>&1; then
    log_error "CN2 (port $CN2_PORT) 不可达"
    exit 1
fi
log_info "CN1 和 CN2 均可达"

# =============================================================================
log_step "Step 2: 准备测试数据库"
# =============================================================================

mysql_cn1 "DROP DATABASE IF EXISTS $DB_NAME;"
mysql_cn1 "CREATE DATABASE $DB_NAME;"
log_info "数据库 $DB_NAME 已创建"

# 在 CN2 上先创建一个表，让 mo_increment_columns 的锁表绑定到 CN2
# 这样 CN2 是锁表的 owner，CN1 的事务是 remote holder
# CN2 做 orphan detection 时需要 RPC 到 CN1 验证事务
log_info "在 CN2 上创建 seed 表（让锁表绑定到 CN2）..."
mysql_cn2 "USE $DB_NAME; CREATE TABLE seed_t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT);"
mysql_cn2 "USE $DB_NAME; INSERT INTO seed_t1 (v) VALUES (1);"
log_info "seed 表已创建"

# =============================================================================
log_step "Step 3: 注入 fault points"
# =============================================================================

mysql_cn1 "SELECT enable_fault_injection();"

# 1. incrservice_after_for_update: CN1 的 store.Allocate 获取 FOR UPDATE 锁后 sleep
#    这让 CN1 持有 mo_increment_columns 的 lockservice 行锁不释放
#    同时阻塞 allocator.run() goroutine
#    freq=1:1:: 表示只触发一次（第一个 CREATE TABLE）
mysql_cn1 "SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_after_for_update#1:1::#sleep#600##false');"
log_info "fault point 已注入：incrservice_after_for_update (sleep 600s, 触发一次)"

# 2. lockservice_get_active_txn_hang: CN1 的 handleGetActiveTxn sleep 30s
#    defaultRPCTimeout = 10s，所以 CN2 的 RPC 一定超时
#    freq=::: 表示每次都触发
mysql_cn1 "SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lockservice_get_active_txn_hang#:::#sleep#30##false');"
log_info "fault point 已注入：lockservice_get_active_txn_hang (sleep 30s, 每次触发)"

log_info ""
log_info "注入的 fault points:"
log_info "  1. CN1 的 store.Allocate 获取 FOR UPDATE 锁后 sleep 600s（持锁不释放）"
log_info "  2. CN1 的 handleGetActiveTxn sleep 30s（RPC 超时 > defaultRPCTimeout 10s）"

# =============================================================================
log_step "Step 4: 在 CN1 上执行 CREATE TABLE（获取锁后卡住）"
# =============================================================================

log_info "在 CN1 上执行 CREATE TABLE t_cn1（预期卡在 store.Allocate sleep）..."

mysql -h "$CN1_HOST" -P "$CN1_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names 2>/dev/null \
    -e "USE $DB_NAME; CREATE TABLE t_cn1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, val INT);" &
LEAK_TXN_PID=$!

# 等待 CN1 获取锁并进入 sleep
sleep 5

if kill -0 "$LEAK_TXN_PID" 2>/dev/null; then
    log_info "CN1 的 CREATE TABLE 正在执行（卡在 incrservice_after_for_update sleep）"
    log_info "CN1 持有 mo_increment_columns 的 lockservice Exclusive 行锁"
else
    log_error "CN1 的 CREATE TABLE 已结束（fault point 可能没触发）"
    log_error "请确认编译的 binary 包含 incrservice_after_for_update fault point"
    exit 1
fi

# =============================================================================
log_step "Step 5: 在 CN2 上执行 CREATE TABLE（触发锁等待 + orphan detection）"
# =============================================================================

log_info "在 CN2 上执行 CREATE TABLE t_cn2..."
log_warn "预期：CN2 的 store.Allocate 需要 mo_increment_columns 行锁"
log_warn "      → 发现 CN1 事务是 holder → 等待"
log_warn "      → 等待 >1 分钟 → checkOrphan → isValidRemoteTxn"
log_warn "      → Method_GetActiveTxn RPC 到 CN1 → 超时（fault point sleep 30s > RPC timeout 10s）"
log_warn "      → isRetryError(context deadline exceeded) → true ← BUG"
log_warn "      → 认为 CN1 事务有效 → 继续等待 → 永远不释放"
echo ""

START_TIME=$(date +%s)
mysql -h "$CN2_HOST" -P "$CN2_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" --skip-column-names 2>/dev/null \
    -e "USE $DB_NAME; CREATE TABLE t_cn2 (id BIGINT AUTO_INCREMENT PRIMARY KEY, val INT);" &
VICTIM_PID=$!

# =============================================================================
log_step "Step 6: 监控日志，等待 orphan detection 触发"
# =============================================================================

log_info "等待 CN2 的 orphan detection 触发..."
log_info "  - lockservice 每 5 秒检查一次 blocked waiters"
log_info "  - 等待超过 1 分钟后触发 checkOrphan"
log_info "  - checkOrphan 调用 isValidRemoteTxn → RPC 到 CN1"
log_info "  - RPC 超时 → isRetryError bug → 锁不释放"
echo ""

# 后台监控 CN2 日志
(docker exec "$CN2_CONTAINER" tail -f /logs/cn2.log 2>/dev/null || true) | \
    grep --line-buffered -E "wait too long|failed to valid txn|found orphans" | \
    while IFS= read -r line; do
        echo -e "  ${YELLOW}[CN2 LOG]${NC} $line"
    done &
LOG_MONITOR_PID=$!

# 等待并检查
WAIT_SECONDS=300  # 最多等 5 分钟
INTERVAL=15
ELAPSED=0

while [ $ELAPSED -lt $WAIT_SECONDS ]; do
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))

    # 检查 victim 是否完成
    if ! kill -0 "$VICTIM_PID" 2>/dev/null; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        wait "$VICTIM_PID" 2>/dev/null
        EXIT_CODE=$?

        echo ""
        if [ $EXIT_CODE -eq 0 ]; then
            log_info "✅ CN2 的 CREATE TABLE 完成！耗时 ${DURATION} 秒"
            log_info "这说明 isRetryError bug 已修复（orphan detection 正确释放了锁）"
        else
            log_warn "CN2 的 CREATE TABLE 失败 (exit=$EXIT_CODE)，耗时 ${DURATION} 秒"
        fi
        VICTIM_PID=""
        exit 0
    fi

    log_info "[${ELAPSED}s] CN2 的 CREATE TABLE 仍在阻塞..."

    # 检查日志计数
    VALID_FAIL=$(docker exec "$CN2_CONTAINER" grep -c "failed to valid txn" /logs/cn2.log 2>/dev/null || echo "0")
    WAIT_LONG=$(docker exec "$CN2_CONTAINER" grep -c "wait too long" /logs/cn2.log 2>/dev/null || echo "0")

    if [ "$VALID_FAIL" -gt 0 ] || [ "$WAIT_LONG" -gt 0 ]; then
        log_info "  CN2 日志计数: failed-to-valid-txn=$VALID_FAIL, wait-too-long=$WAIT_LONG"
    fi

    # 90 秒后如果还在阻塞，基本确认 bug 已复现
    if [ $ELAPSED -ge 90 ] && [ "$VALID_FAIL" -gt 0 ]; then
        echo ""
        log_error "============================================"
        log_error "🐛 isRetryError BUG 已复现！"
        log_error "============================================"
        echo ""
        log_error "CN2 的 CREATE TABLE 已阻塞 ${ELAPSED} 秒"
        log_error "CN2 日志显示 ${VALID_FAIL} 次 'failed to valid txn'（RPC 超时）"
        log_error "但 isRetryError 返回 true，锁永远不释放"
        echo ""
        log_info "根因：lock_table_remote.go:326 isRetryError()"
        log_info "  context deadline exceeded 不是 ErrBackendClosed/ErrBackendCannotConnect"
        log_info "  → 返回 true → isValidRemoteTxn 返回 true → 锁不释放"
        echo ""
        log_info "查看详细日志："
        log_info "  docker exec $CN2_CONTAINER grep 'failed to valid txn' /logs/cn2.log | tail -5"
        log_info "  docker exec $CN2_CONTAINER grep 'wait too long' /logs/cn2.log | tail -3"
        echo ""
        log_info "按 Ctrl+C 退出（cleanup 会自动执行）"

        # 继续等待让用户观察
        wait "$VICTIM_PID" 2>/dev/null || true
        exit 0
    fi
done

# 超时
if [ -n "${VICTIM_PID:-}" ] && kill -0 "$VICTIM_PID" 2>/dev/null; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    log_error "等待 ${WAIT_SECONDS} 秒后超时，CN2 的 CREATE TABLE 仍在阻塞"
    log_error "BUG 已复现（${DURATION} 秒）"
fi

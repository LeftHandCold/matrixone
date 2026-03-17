#!/bin/bash
# 复现 lockWithRetry 无限循环 Bug
#
# 前置条件：
#   1. 已编译包含 MO_REPRO_REMOTE_LOCK_TIMEOUT 环境变量检查的二进制
#   2. 多 CN 集群已启动（CN1: 16001, CN2: 16002）
#
# 原理：
#   在 remoteLockTable.lock() 中，如果环境变量 MO_REPRO_REMOTE_LOCK_TIMEOUT=1，
#   则跳过 client.Send 直接返回 context.DeadlineExceeded。
#   handleError 把 DeadlineExceeded 转成 BackendCannotConnect，
#   canRetryLock 看到 BackendCannotConnect → 继续重试 → 无限循环。
#
# 关键：
#   环境变量只在 remoteLockTable.lock() 中检查，所以必须让 CN1 去锁
#   一个绑定在 CN2 上的表。方法是：
#     1. CN2 创建 seed 表（seed_t1 的锁表绑定到 CN2）
#     2. CN1 对 seed_t1 执行 INSERT（需要锁 seed_t1 的行 → remote lock → 触发）
#
# 用法：
#   # 方式 1：集群已经带环境变量启动
#   bash doc/reproduce_lockwithretry_infinite_loop.sh
#
#   # 方式 2：集群没有环境变量，脚本自动重启 CN1
#   bash doc/reproduce_lockwithretry_infinite_loop.sh --restart-cn1

set -euo pipefail

CN1_PORT=16001
CN2_PORT=16002
MYSQL_USER=root
MYSQL_PASS=111
DB_NAME=test_retry_loop
COMPOSE_DIR="etc/docker-multi-cn-local-disk"
RESTART_CN1=false

# 解析参数
for arg in "$@"; do
    case $arg in
        --restart-cn1)
            RESTART_CN1=true
            ;;
    esac
done

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
    if [[ -n "${INSERT_PID:-}" ]]; then
        kill "$INSERT_PID" 2>/dev/null || true
        wait "$INSERT_PID" 2>/dev/null || true
    fi
    mysql_cn2 "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
}
trap cleanup EXIT

echo "============================================"
echo "  lockWithRetry 无限循环 Bug 复现"
echo "  (环境变量方案: MO_REPRO_REMOTE_LOCK_TIMEOUT)"
echo "============================================"
echo ""

# Step 0: 确保 CN1 有环境变量
echo -e "${YELLOW}[Step 0] 检查 CN1 环境变量...${NC}"
ENV_CHECK=$(docker exec mo-cn1 printenv MO_REPRO_REMOTE_LOCK_TIMEOUT 2>/dev/null || echo "")
if [[ "$ENV_CHECK" != "1" ]]; then
    if [[ "$RESTART_CN1" == "true" ]]; then
        echo -e "  CN1 未设置环境变量，正在重启 CN1..."
        # 停止 CN1
        docker stop mo-cn1 2>/dev/null || true
        docker rm mo-cn1 2>/dev/null || true
        # 用 docker compose 重启，注入环境变量
        echo -e "  重启中（MO_REPRO_REMOTE_LOCK_TIMEOUT=1）..."
        (cd "$COMPOSE_DIR" && MO_REPRO_REMOTE_LOCK_TIMEOUT=1 docker compose up -d mo-cn1) 2>&1 | head -5
        echo -e "  等待 CN1 就绪（30 秒）..."
        sleep 30
        # 再次检查
        ENV_CHECK=$(docker exec mo-cn1 printenv MO_REPRO_REMOTE_LOCK_TIMEOUT 2>/dev/null || echo "")
        if [[ "$ENV_CHECK" != "1" ]]; then
            echo -e "${RED}  ❌ 重启后仍未设置环境变量${NC}"
            exit 1
        fi
        echo -e "${GREEN}  ✅ CN1 已重启并设置 MO_REPRO_REMOTE_LOCK_TIMEOUT=1${NC}"
    else
        echo -e "${RED}  ❌ CN1 未设置 MO_REPRO_REMOTE_LOCK_TIMEOUT=1${NC}"
        echo ""
        echo -e "  方式 1：重启整个集群"
        echo -e "    cd $COMPOSE_DIR"
        echo -e "    docker compose --profile matrixone down"
        echo -e "    MO_REPRO_REMOTE_LOCK_TIMEOUT=1 IMAGE_NAME=matrixorigin/matrixone:local \\"
        echo -e "      docker compose --profile matrixone up -d"
        echo -e "    sleep 30"
        echo ""
        echo -e "  方式 2：让脚本自动重启 CN1（不影响其他节点）"
        echo -e "    bash doc/reproduce_lockwithretry_infinite_loop.sh --restart-cn1"
        echo ""
        exit 1
    fi
else
    echo -e "${GREEN}  ✅ CN1 已设置 MO_REPRO_REMOTE_LOCK_TIMEOUT=1${NC}"
fi

# Step 1: 清理旧数据
echo -e "${YELLOW}[Step 1] 清理旧数据...${NC}"
mysql_cn2 "DROP DATABASE IF EXISTS $DB_NAME;" || true

# Step 2: CN2 创建 seed 表（让 seed_t1 的锁表绑定到 CN2）
echo -e "${YELLOW}[Step 2] CN2 创建 seed 表（让 seed_t1 的锁表绑定到 CN2）...${NC}"
mysql_cn2 "CREATE DATABASE IF NOT EXISTS $DB_NAME;"
mysql_cn2 "USE $DB_NAME; CREATE TABLE seed_t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT);"
mysql_cn2 "USE $DB_NAME; INSERT INTO seed_t1 (v) VALUES (1);"
echo -e "${GREEN}  ✅ seed_t1 已创建，seed_t1 的锁表绑定到 CN2${NC}"
echo -e "  （注意：mo_increment_columns 的锁表仍在 CN1 上，不受影响）"

# Step 3: CN1 后台执行 INSERT（需要锁 seed_t1 → remote lock 到 CN2 → 触发环境变量）
echo -e "${YELLOW}[Step 3] CN1 执行 INSERT INTO seed_t1（后台）...${NC}"
echo -e "  seed_t1 的锁表在 CN2 上 → CN1 走 remoteLockTable.lock() → 环境变量触发"
echo -e "  remoteLockTable.lock() 跳过 client.Send，直接返回 context.DeadlineExceeded"
mysql -h 127.0.0.1 -P "$CN1_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
    -e "USE $DB_NAME; INSERT INTO seed_t1 (v) VALUES (100);" \
    2>/dev/null &
INSERT_PID=$!
echo -e "  INSERT PID: $INSERT_PID"

# Step 4: 监控日志
echo -e "${YELLOW}[Step 4] 监控 CN1 日志（等待 60 秒）...${NC}"
echo ""

WAIT_SECONDS=60
INTERVAL=5

for ((i=1; i<=WAIT_SECONDS/INTERVAL; i++)); do
    sleep "$INTERVAL"

    # 检查 INSERT 是否还在运行
    if ! kill -0 "$INSERT_PID" 2>/dev/null; then
        echo -e "${RED}  ❌ INSERT 已退出（不应该退出）${NC}"
        wait "$INSERT_PID" 2>/dev/null
        EXIT_CODE=$?
        echo -e "${RED}  退出码: $EXIT_CODE${NC}"
        echo -e "${RED}  Bug 未复现 — INSERT 没有卡住${NC}"
        echo ""
        echo -e "  最后几条 CN1 日志："
        docker logs mo-cn1 2>&1 | tail -20
        exit 1
    fi

    # 检查 CN1 日志中的诊断信息
    REPRO_COUNT=$(docker logs mo-cn1 2>&1 | grep -c "BUG REPRO: injecting context.DeadlineExceeded" || true)
    RETRY_COUNT=$(docker logs mo-cn1 2>&1 | grep -c "lockWithRetry" || true)
    CTX_EXPIRED=$(docker logs mo-cn1 2>&1 | grep "lockWithRetry" | grep -c "ctx.Err=context deadline exceeded" || true)
    CTX_NIL=$(docker logs mo-cn1 2>&1 | grep "lockWithRetry" | grep -c "ctx.Err=<nil>" || true)
    REMOTE_LOCK_FAIL=$(docker logs mo-cn1 2>&1 | grep -c "failed to lock on remote" || true)
    echo -e "  [${i}/$((WAIT_SECONDS/INTERVAL))] INSERT 仍在运行 | 注入: $REPRO_COUNT | retry: $RETRY_COUNT | ctx nil: $CTX_NIL | ctx expired: $CTX_EXPIRED | remote fail: $REMOTE_LOCK_FAIL"
done

echo ""

# Step 5: 判定结果
REPRO_COUNT=$(docker logs mo-cn1 2>&1 | grep -c "BUG REPRO: injecting context.DeadlineExceeded" || true)
RETRY_COUNT=$(docker logs mo-cn1 2>&1 | grep -c "lockWithRetry" || true)
CTX_EXPIRED=$(docker logs mo-cn1 2>&1 | grep "lockWithRetry" | grep -c "ctx.Err=context deadline exceeded" || true)

if kill -0 "$INSERT_PID" 2>/dev/null && [[ "$REPRO_COUNT" -gt 3 ]]; then
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}  🐛 lockWithRetry 无限循环 Bug 已复现！${NC}"
    echo -e "${RED}============================================${NC}"
    echo ""
    echo -e "  CN1 的 INSERT 已阻塞 ${WAIT_SECONDS} 秒"
    echo -e "  环境变量注入了 ${REPRO_COUNT} 次 context.DeadlineExceeded"
    echo -e "  lockWithRetry 重试了 ${RETRY_COUNT} 次"
    echo ""
    if [[ "$CTX_EXPIRED" -gt 0 ]]; then
        echo -e "  ${RED}铁证：ctx.Err=context deadline exceeded 但循环仍在继续${NC}"
        echo -e "  原因：handleError 把 context.DeadlineExceeded 转成 ErrBackendCannotConnect"
        echo -e "        canRetryLock 看到 BackendCannotConnect → 继续重试"
        echo -e "        canRetryLock 不检查 ctx.Err()，所以永远不会停止"
    else
        echo -e "  ctx 还未过期，但 retry 已在持续增长"
        echo -e "  等 ctx 过期后，循环仍不会停止"
    fi
    echo ""
    echo -e "  ${YELLOW}修复方式：lockWithRetry 或 canRetryLock 检查 ctx.Err()${NC}"
    echo ""
    echo -e "  最后几条诊断日志："
    docker logs mo-cn1 2>&1 | grep -E "(BUG REPRO|lockWithRetry)" | tail -10
    exit 0
elif kill -0 "$INSERT_PID" 2>/dev/null; then
    echo -e "${YELLOW}  ⚠️ INSERT 仍在运行但没有看到 BUG REPRO 日志${NC}"
    echo -e "  可能锁表没有绑定到 CN2（CN1 用了 localLockTable）"
    echo ""
    echo -e "  检查 CN1 日志中的 BUG REPRO 信息："
    docker logs mo-cn1 2>&1 | grep "BUG REPRO" | tail -5
    echo ""
    echo -e "  检查 CN1 日志中的 remote lock 信息："
    docker logs mo-cn1 2>&1 | grep -i "remote.*lock" | tail -10
    echo ""
    echo -e "  检查 CN1 日志中的 bind 信息："
    docker logs mo-cn1 2>&1 | grep "bind created" | tail -10
    echo ""
    echo -e "  检查 CN2 日志中的 bind 信息："
    docker logs mo-cn2 2>&1 | grep "bind created" | tail -10
    echo ""
    echo -e "  最后几条 CN1 日志："
    docker logs mo-cn1 2>&1 | tail -20
    exit 1
else
    echo -e "${YELLOW}  ⚠️ INSERT 已退出${NC}"
    echo -e "  BUG REPRO count: $REPRO_COUNT | Retry count: $RETRY_COUNT"
    echo ""
    echo -e "  最后几条 CN1 日志："
    docker logs mo-cn1 2>&1 | tail -20
    exit 1
fi

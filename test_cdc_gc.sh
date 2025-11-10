#!/bin/bash

# CDC GC 功能测试脚本
# 测试场景：
# 1. 多数据库、多表的CDC保护
# 2. 最小watermark策略
# 3. 数据保护边界测试
# 4. 应该被GC的数据正确删除
# 5. 不应该被GC的数据正确保护

set -e

# 配置参数
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-6001}"
DB_USER="${DB_USER:-dump}"
DB_PASS="${DB_PASS:-111}"
ACCOUNT_ID="${ACCOUNT_ID:-0}"

# 检测MySQL客户端
MYSQL_CMD=""
if command -v mysql &> /dev/null; then
    MYSQL_CMD="mysql"
elif command -v mo &> /dev/null; then
    MYSQL_CMD="mo"
else
    echo "错误: 未找到mysql或mo客户端，请安装MySQL客户端"
    exit 1
fi

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 执行SQL函数
execute_sql() {
    local sql="$1"
    local result=""
    if [ "$MYSQL_CMD" = "mo" ]; then
        result=$(echo "$sql" | mo -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_USER}" -p "${DB_PASS}" 2>/dev/null || echo "")
    else
        result=$(mysql -h"${DB_HOST}" -P"${DB_PORT}" -u"${DB_USER}" -p"${DB_PASS}" -sN -e "$sql" 2>/dev/null || echo "")
    fi
    echo "$result"
}

# 执行SQL并显示结果
execute_sql_verbose() {
    local sql="$1"
    log_info "执行SQL: $sql"
    if [ "$MYSQL_CMD" = "mo" ]; then
        echo "$sql" | mo -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_USER}" -p "${DB_PASS}" 2>/dev/null || {
            log_error "SQL执行失败: $sql"
            return 1
        }
    else
        mysql -h"${DB_HOST}" -P"${DB_PORT}" -u"${DB_USER}" -p"${DB_PASS}" -e "$sql" 2>/dev/null || {
            log_error "SQL执行失败: $sql"
            return 1
        }
    fi
}

# 等待checkpoint完成
wait_for_checkpoint() {
    local max_wait=${1:-60}
    local waited=0
    log_info "等待checkpoint完成（最多${max_wait}秒）..."
    
    while [ $waited -lt $max_wait ]; do
        local pending=$(execute_sql "SELECT COUNT(*) FROM mo_catalog.mo_checkpoints WHERE status != 'Finished' LIMIT 1" || echo "0")
        if [ "$pending" = "0" ] || [ -z "$pending" ]; then
            log_info "Checkpoint已完成"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    
    log_warn "Checkpoint等待超时"
    return 1
}

# 等待GC执行
wait_for_gc() {
    local max_wait=${1:-120}
    local waited=0
    log_info "等待GC执行（最多${max_wait}秒）..."
    
    # 等待一段时间让GC有机会执行
    sleep 10
    
    # 检查GC是否执行过
    local gc_count=$(execute_sql "SELECT COUNT(*) FROM mo_catalog.mo_gc_objects LIMIT 1" || echo "0")
    log_info "GC对象计数: $gc_count"
    
    # 等待GC窗口
    while [ $waited -lt $max_wait ]; do
        sleep 5
        waited=$((waited + 5))
        if [ $((waited % 30)) -eq 0 ]; then
            log_info "已等待 ${waited} 秒，继续等待GC..."
        fi
    done
    
    log_info "GC等待完成"
}

# 获取当前时间戳（用于watermark）
get_current_timestamp() {
    # 返回格式: 2024-01-01 12:00:00.000000
    date +"%Y-%m-%d %H:%M:%S.%6N"
}

# 获取相对时间戳（用于测试）
get_relative_timestamp() {
    local offset_minutes=$1
    local timestamp=""
    
    # 尝试不同的date命令格式（Linux和macOS）
    if date -u -d "${offset_minutes} minutes ago" +"%Y-%m-%d %H:%M:%S.%6N" &>/dev/null; then
        # Linux格式
        timestamp=$(date -u -d "${offset_minutes} minutes ago" +"%Y-%m-%d %H:%M:%S.%6N")
    elif date -u -v-${offset_minutes}M +"%Y-%m-%d %H:%M:%S.%6N" &>/dev/null; then
        # macOS格式
        timestamp=$(date -u -v-${offset_minutes}M +"%Y-%m-%d %H:%M:%S.%6N")
    else
        # 如果都失败，使用Python计算
        timestamp=$(python3 -c "from datetime import datetime, timedelta; print((datetime.utcnow() - timedelta(minutes=${offset_minutes})).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])" 2>/dev/null || \
                    date +"%Y-%m-%d %H:%M:%S.%6N")
    fi
    
    echo "$timestamp"
}

# 测试场景1: 基本CDC保护测试
test_basic_cdc_protection() {
    log_info "========== 测试场景1: 基本CDC保护 =========="
    
    local test_db="test_cdc_db1"
    local test_table="test_table1"
    local task_id="test_task_1"
    
    # 创建测试数据库和表
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    
    # 插入测试数据（不同时间戳）
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (1, 'data1'), (2, 'data2'), (3, 'data3')"
    
    # 等待数据flush
    sleep 3
    
    # 获取当前时间戳作为watermark
    local watermark_ts=$(get_relative_timestamp 5)  # 5分钟前的时间戳
    
    # 插入CDC watermark记录
    log_info "插入CDC watermark: ${watermark_ts}"
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table}', '${watermark_ts}')"
    
    # 等待checkpoint
    wait_for_checkpoint 30
    
    # 插入更多数据（在watermark之后）
    log_info "插入watermark之后的数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (4, 'data4'), (5, 'data5')"
    
    # 等待checkpoint和GC
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证数据是否还在（应该被保护）
    local count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table}")
    log_info "表 ${test_table} 中的数据行数: ${count}"
    
    if [ "$count" -ge 3 ]; then
        log_info "✓ 测试通过: 数据被正确保护"
    else
        log_error "✗ 测试失败: 数据被错误删除"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景1测试完成\n"
}

# 测试场景2: 最小watermark策略
test_min_watermark_strategy() {
    log_info "========== 测试场景2: 最小watermark策略 =========="
    
    local test_db="test_cdc_db2"
    local test_table1="test_table1"
    local test_table2="test_table2"
    local task_id="test_task_2"
    
    # 创建测试数据库和表
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table1} (id INT PRIMARY KEY, name VARCHAR(100))"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table2} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 插入测试数据
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table1} (id, name) VALUES (1, 't1_data1'), (2, 't1_data2')"
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table2} (id, name) VALUES (1, 't2_data1'), (2, 't2_data2')"
    
    wait_for_checkpoint 30
    
    # 插入不同时间戳的watermark（测试最小watermark策略）
    local watermark1_ts=$(get_relative_timestamp 10)  # 10分钟前（更早）
    local watermark2_ts=$(get_relative_timestamp 5)   # 5分钟前（更晚）
    
    log_info "插入CDC watermark: table1=${watermark1_ts}, table2=${watermark2_ts}"
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table1}', '${watermark1_ts}')"
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table2}', '${watermark2_ts}')"
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：应该使用最小watermark（watermark1_ts），所以两个表的数据都应该被保护
    local count1=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table1}")
    local count2=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table2}")
    
    log_info "表 ${test_table1} 数据行数: ${count1}"
    log_info "表 ${test_table2} 数据行数: ${count2}"
    
    if [ "$count1" -ge 2 ] && [ "$count2" -ge 2 ]; then
        log_info "✓ 测试通过: 最小watermark策略工作正常"
    else
        log_error "✗ 测试失败: 最小watermark策略异常"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景2测试完成\n"
}

# 测试场景3: 多数据库CDC保护
test_multi_database_protection() {
    log_info "========== 测试场景3: 多数据库CDC保护 =========="
    
    local test_db1="test_cdc_db3_1"
    local test_db2="test_cdc_db3_2"
    local test_db3="test_cdc_db3_3"
    local test_table="test_table"
    local task_id1="test_task_3_1"
    local task_id2="test_task_3_2"
    
    # 创建多个测试数据库
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db1}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db2}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db3}"
    
    execute_sql_verbose "USE ${test_db1}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    execute_sql_verbose "USE ${test_db2}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    execute_sql_verbose "USE ${test_db3}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 插入测试数据
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db1}; INSERT INTO ${test_table} (id, name) VALUES (1, 'db1_data1'), (2, 'db1_data2')"
    execute_sql_verbose "USE ${test_db2}; INSERT INTO ${test_table} (id, name) VALUES (1, 'db2_data1'), (2, 'db2_data2')"
    execute_sql_verbose "USE ${test_db3}; INSERT INTO ${test_table} (id, name) VALUES (1, 'db3_data1'), (2, 'db3_data2')"
    
    wait_for_checkpoint 30
    
    # 只为db1和db2设置CDC保护
    local watermark_ts=$(get_relative_timestamp 5)
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id1}', '${test_db1}', '${test_table}', '${watermark_ts}')"
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id2}', '${test_db2}', '${test_table}', '${watermark_ts}')"
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：db1和db2应该被保护，db3可能被GC
    local count1=$(execute_sql "USE ${test_db1}; SELECT COUNT(*) FROM ${test_table}")
    local count2=$(execute_sql "USE ${test_db2}; SELECT COUNT(*) FROM ${test_table}")
    local count3=$(execute_sql "USE ${test_db3}; SELECT COUNT(*) FROM ${test_table}")
    
    log_info "数据库 ${test_db1} 数据行数: ${count1}"
    log_info "数据库 ${test_db2} 数据行数: ${count2}"
    log_info "数据库 ${test_db3} 数据行数: ${count3}"
    
    if [ "$count1" -ge 2 ] && [ "$count2" -ge 2 ]; then
        log_info "✓ 测试通过: 多数据库CDC保护工作正常"
    else
        log_error "✗ 测试失败: 多数据库CDC保护异常"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id IN ('${task_id1}', '${task_id2}')"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db1}"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db2}"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db3}"
    
    log_info "场景3测试完成\n"
}

# 测试场景4: 数据边界测试（应该被GC的数据）
test_data_boundary_gc() {
    log_info "========== 测试场景4: 数据边界测试（应该被GC） =========="
    
    local test_db="test_cdc_db4"
    local test_table="test_table"
    local task_id="test_task_4"
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    
    # 插入一些旧数据（在watermark之前）
    log_info "插入旧数据（应该被GC）..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (1, 'old_data1'), (2, 'old_data2')"
    
    wait_for_checkpoint 30
    
    # 设置一个较新的watermark（旧数据应该被GC）
    local watermark_ts=$(get_relative_timestamp 1)  # 1分钟前
    
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table}', '${watermark_ts}')"
    
    # 插入新数据（在watermark之后，应该被保护）
    log_info "插入新数据（应该被保护）..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (3, 'new_data1'), (4, 'new_data2')"
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：新数据应该还在，旧数据可能被GC
    local count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table}")
    local new_data_count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table} WHERE id >= 3")
    
    log_info "表总数据行数: ${count}"
    log_info "新数据行数（id>=3）: ${new_data_count}"
    
    if [ "$new_data_count" -ge 2 ]; then
        log_info "✓ 测试通过: 新数据被正确保护"
    else
        log_error "✗ 测试失败: 新数据被错误删除"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景4测试完成\n"
}

# 测试场景5: 空watermark测试（保护所有数据）
test_empty_watermark() {
    log_info "========== 测试场景5: 空watermark测试 =========="
    
    local test_db="test_cdc_db5"
    local test_table="test_table"
    local task_id="test_task_5"
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 插入测试数据
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (1, 'data1'), (2, 'data2'), (3, 'data3')"
    
    wait_for_checkpoint 30
    
    # 插入空watermark（应该保护所有数据）
    log_info "插入空watermark..."
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table}', '')"
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：所有数据应该被保护
    local count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table}")
    
    log_info "表数据行数: ${count}"
    
    if [ "$count" -ge 3 ]; then
        log_info "✓ 测试通过: 空watermark保护所有数据"
    else
        log_error "✗ 测试失败: 空watermark未正确保护数据"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景5测试完成\n"
}

# 测试场景6: watermark更新测试
test_watermark_update() {
    log_info "========== 测试场景6: watermark更新测试 =========="
    
    local test_db="test_cdc_db6"
    local test_table="test_table"
    local task_id="test_task_6"
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 插入测试数据
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (1, 'data1'), (2, 'data2')"
    
    wait_for_checkpoint 30
    
    # 设置初始watermark
    local watermark1_ts=$(get_relative_timestamp 10)
    execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${test_table}', '${watermark1_ts}')"
    
    wait_for_checkpoint 30
    
    # 更新watermark（更早的时间）
    local watermark2_ts=$(get_relative_timestamp 15)
    log_info "更新watermark: ${watermark1_ts} -> ${watermark2_ts}"
    execute_sql_verbose "UPDATE mo_catalog.mo_cdc_watermark SET watermark='${watermark2_ts}' WHERE task_id='${task_id}' AND db_name='${test_db}' AND table_name='${test_table}'"
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：数据应该被保护（使用最小watermark）
    local count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${test_table}")
    
    log_info "表数据行数: ${count}"
    
    if [ "$count" -ge 2 ]; then
        log_info "✓ 测试通过: watermark更新工作正常"
    else
        log_error "✗ 测试失败: watermark更新异常"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景6测试完成\n"
}

# 测试场景7: 并发多表CDC保护
test_concurrent_multi_table() {
    log_info "========== 测试场景7: 并发多表CDC保护 =========="
    
    local test_db="test_cdc_db7"
    local task_id="test_task_7"
    local num_tables=5
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    
    # 创建多个表
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${table_name} (id INT PRIMARY KEY, name VARCHAR(100), data VARCHAR(200))"
        
        # 为每个表插入数据
        for j in $(seq 1 3); do
            execute_sql_verbose "USE ${test_db}; INSERT INTO ${table_name} (id, name, data) VALUES ($((i*10+j)), 'name${j}', 'data${j}')"
        done
    done
    
    wait_for_checkpoint 30
    
    # 为每个表设置不同的watermark
    local base_minutes=5
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        local watermark_ts=$(get_relative_timestamp $((base_minutes + i)))
        execute_sql_verbose "INSERT INTO mo_catalog.mo_cdc_watermark (account_id, task_id, db_name, table_name, watermark) VALUES (${ACCOUNT_ID}, '${task_id}', '${test_db}', '${table_name}', '${watermark_ts}')"
    done
    
    wait_for_checkpoint 30
    wait_for_gc 60
    
    # 验证：所有表的数据应该被保护（使用最小watermark）
    local all_protected=true
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        local count=$(execute_sql "USE ${test_db}; SELECT COUNT(*) FROM ${table_name}")
        log_info "表 ${table_name} 数据行数: ${count}"
        
        if [ "$count" -lt 3 ]; then
            all_protected=false
            log_error "表 ${table_name} 数据被错误删除"
        fi
    done
    
    if [ "$all_protected" = true ]; then
        log_info "✓ 测试通过: 并发多表CDC保护工作正常"
    else
        log_error "✗ 测试失败: 并发多表CDC保护异常"
        return 1
    fi
    
    # 清理
    execute_sql_verbose "DELETE FROM mo_catalog.mo_cdc_watermark WHERE task_id='${task_id}'"
    execute_sql_verbose "DROP DATABASE IF EXISTS ${test_db}"
    
    log_info "场景7测试完成\n"
}

# 主函数
main() {
    log_info "开始CDC GC功能测试..."
    log_info "数据库连接: ${DB_USER}@${DB_HOST}:${DB_PORT}"
    log_info "账户ID: ${ACCOUNT_ID}"
    echo ""
    
    # 检查数据库连接
    if ! execute_sql "SELECT 1" > /dev/null 2>&1; then
        log_error "无法连接到数据库，请检查连接参数"
        exit 1
    fi
    
    # 检查必要的表是否存在
    if ! execute_sql "SELECT COUNT(*) FROM mo_catalog.mo_cdc_watermark LIMIT 1" > /dev/null 2>&1; then
        log_error "mo_cdc_watermark表不存在，请确保CDC功能已启用"
        exit 1
    fi
    
    local failed_tests=0
    
    # 运行所有测试场景
    test_basic_cdc_protection || failed_tests=$((failed_tests + 1))
    test_min_watermark_strategy || failed_tests=$((failed_tests + 1))
    test_multi_database_protection || failed_tests=$((failed_tests + 1))
    test_data_boundary_gc || failed_tests=$((failed_tests + 1))
    test_empty_watermark || failed_tests=$((failed_tests + 1))
    test_watermark_update || failed_tests=$((failed_tests + 1))
    test_concurrent_multi_table || failed_tests=$((failed_tests + 1))
    
    # 测试总结
    echo ""
    log_info "========== 测试总结 =========="
    if [ $failed_tests -eq 0 ]; then
        log_info "✓ 所有测试通过！"
        exit 0
    else
        log_error "✗ 有 ${failed_tests} 个测试失败"
        exit 1
    fi
}

# 运行主函数
main "$@"


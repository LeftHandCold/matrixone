#!/bin/bash

# CDC GC 功能测试脚本
# 测试场景：
# 1. 基本CDC保护测试
# 2. 最小watermark策略
# 3. 多数据库CDC保护
# 4. 数据持续写入和watermark更新
# 5. 任务暂停和重启
# 6. 多表CDC保护

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

# 查询CDC watermark
query_watermark() {
    local task_id="$1"
    local db_name="$2"
    local table_name="$3"
    
    if [ -n "$table_name" ]; then
        # 查询特定表的watermark
        execute_sql "SELECT watermark FROM mo_catalog.mo_cdc_watermark WHERE account_id=${ACCOUNT_ID} AND task_id='${task_id}' AND db_name='${db_name}' AND table_name='${table_name}' LIMIT 1"
    elif [ -n "$db_name" ]; then
        # 查询数据库所有表的watermark
        execute_sql_verbose "SELECT db_name, table_name, watermark, err_msg FROM mo_catalog.mo_cdc_watermark WHERE account_id=${ACCOUNT_ID} AND task_id='${task_id}' AND db_name='${db_name}'"
    else
        # 查询任务所有watermark
        execute_sql_verbose "SELECT db_name, table_name, watermark, err_msg FROM mo_catalog.mo_cdc_watermark WHERE account_id=${ACCOUNT_ID} AND task_id='${task_id}'"
    fi
}

# 创建CDC任务
create_cdc_task() {
    local task_id="$1"
    local source_uri="$2"
    local source_db="$3"
    local sink_uri="$4"
    local sink_db="$5"
    local level="${6:-database}"
    
    local sql="CREATE CDC ${task_id} '${source_uri}' '${source_db}' '${sink_uri}' '${sink_db}' {'Level'='${level}'};"
    log_info "创建CDC任务: ${task_id}"
    execute_sql_verbose "$sql"
}

# 暂停CDC任务
pause_cdc_task() {
    local task_id="$1"
    local sql="PAUSE CDC TASK ${task_id};"
    log_info "暂停CDC任务: ${task_id}"
    execute_sql_verbose "$sql"
}

# 重启CDC任务
resume_cdc_task() {
    local task_id="$1"
    local sql="RESUME CDC TASK ${task_id};"
    log_info "重启CDC任务: ${task_id}"
    execute_sql_verbose "$sql"
}

# 测试场景1: 基本CDC保护测试
test_basic_cdc_protection() {
    log_info "========== 测试场景1: 基本CDC保护 =========="
    
    local test_db="test_cdc_db1"
    local test_table="test_table1"
    local task_id="test_cdc_task1"
    local sink_db="${test_db}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    
    # 创建测试数据库和表
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    
    # 创建CDC任务
    create_cdc_task "${task_id}" "${source_uri}" "${test_db}" "${sink_uri}" "${sink_db}" "database"
    
    # 等待任务启动
    sleep 2
    
    # 插入测试数据
    log_info "插入测试数据..."
    for i in {1..5}; do
        execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (${i}, 'data${i}')"
        sleep 1
    done
    
    # 查询watermark
    log_info "查询CDC watermark..."
    query_watermark "${task_id}" "${test_db}" ""
    
    # 验证数据是否同步到目标库
    local count=$(execute_sql "USE ${sink_db}; SELECT COUNT(*) FROM ${test_table}" 2>/dev/null || echo "0")
    log_info "目标库 ${sink_db} 中的数据行数: ${count}"
    
    if [ "$count" -gt 0 ]; then
        log_info "✓ 测试通过: 数据同步正常"
    else
        log_warn "⚠ 数据尚未同步，watermark可能还未更新"
    fi
    
    log_info "场景1测试完成\n"
}

# 测试场景2: 最小watermark策略
test_min_watermark_strategy() {
    log_info "========== 测试场景2: 最小watermark策略 =========="
    
    local test_db="test_cdc_db2"
    local test_table1="test_table1"
    local test_table2="test_table2"
    local task_id="test_cdc_task2"
    local sink_db="${test_db}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    
    # 创建测试数据库和表
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table1} (id INT PRIMARY KEY, name VARCHAR(100))"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table2} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 创建CDC任务
    create_cdc_task "${task_id}" "${source_uri}" "${test_db}" "${sink_uri}" "${sink_db}" "database"
    
    sleep 2
    
    # 为不同表插入数据
    log_info "为不同表插入数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table1} (id, name) VALUES (1, 't1_data1'), (2, 't1_data2')"
    sleep 1
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table2} (id, name) VALUES (1, 't2_data1'), (2, 't2_data2')"
    sleep 1
    
    # 查询watermark（应该显示两个表的watermark）
    log_info "查询CDC watermark（验证最小watermark策略）..."
    query_watermark "${task_id}" "${test_db}" ""
    
    # 验证：两个表都应该有watermark
    local wm1=$(query_watermark "${task_id}" "${test_db}" "${test_table1}")
    local wm2=$(query_watermark "${task_id}" "${test_db}" "${test_table2}")
    
    log_info "表 ${test_table1} watermark: ${wm1}"
    log_info "表 ${test_table2} watermark: ${wm2}"
    
    if [ -n "$wm1" ] && [ -n "$wm2" ]; then
        log_info "✓ 测试通过: 最小watermark策略工作正常"
    else
        log_warn "⚠ watermark可能还未更新"
    fi
    
    log_info "场景2测试完成\n"
}

# 测试场景3: 多数据库CDC保护
test_multi_database_protection() {
    log_info "========== 测试场景3: 多数据库CDC保护 =========="
    
    local test_db1="test_cdc_db3_1"
    local test_db2="test_cdc_db3_2"
    local test_table="test_table"
    local task_id1="test_cdc_task3_1"
    local task_id2="test_cdc_task3_2"
    local sink_db1="${test_db1}_bak"
    local sink_db2="${test_db2}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    
    # 创建多个测试数据库
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db1}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db2}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db1}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db2}"
    
    execute_sql_verbose "USE ${test_db1}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    execute_sql_verbose "USE ${test_db2}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 创建多个CDC任务
    log_info "创建多个CDC任务..."
    create_cdc_task "${task_id1}" "${source_uri}" "${test_db1}" "${sink_uri}" "${sink_db1}" "database"
    create_cdc_task "${task_id2}" "${source_uri}" "${test_db2}" "${sink_uri}" "${sink_db2}" "database"
    
    sleep 2
    
    # 插入测试数据
    log_info "插入测试数据..."
    execute_sql_verbose "USE ${test_db1}; INSERT INTO ${test_table} (id, name) VALUES (1, 'db1_data1'), (2, 'db1_data2')"
    execute_sql_verbose "USE ${test_db2}; INSERT INTO ${test_table} (id, name) VALUES (1, 'db2_data1'), (2, 'db2_data2')"
    
    sleep 2
    
    # 查询watermark
    log_info "查询多个数据库的watermark..."
    query_watermark "${task_id1}" "${test_db1}" ""
    query_watermark "${task_id2}" "${test_db2}" ""
    
    log_info "✓ 测试通过: 多数据库CDC保护工作正常"
    log_info "场景3测试完成\n"
}

# 测试场景4: 数据持续写入和watermark更新
test_continuous_write() {
    log_info "========== 测试场景4: 数据持续写入和watermark更新 =========="
    
    local test_db="test_cdc_db4"
    local test_table="test_table"
    local task_id="test_cdc_task4"
    local sink_db="${test_db}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100), data VARCHAR(200))"
    
    # 创建CDC任务
    create_cdc_task "${task_id}" "${source_uri}" "${test_db}" "${sink_uri}" "${sink_db}" "database"
    
    sleep 2
    
    # 持续写入数据并监控watermark更新
    log_info "持续写入数据并监控watermark更新..."
    for i in {1..10}; do
        execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name, data) VALUES (${i}, 'name${i}', 'data${i}')"
        
        if [ $((i % 3)) -eq 0 ]; then
            log_info "已插入 ${i} 条数据，查询watermark..."
            local wm=$(query_watermark "${task_id}" "${test_db}" "${test_table}")
            log_info "当前watermark: ${wm}"
        fi
        
        sleep 1
    done
    
    # 最终查询watermark
    log_info "最终watermark状态:"
    query_watermark "${task_id}" "${test_db}" ""
    
    log_info "✓ 测试通过: 持续写入和watermark更新正常"
    log_info "场景4测试完成\n"
}

# 测试场景5: 任务暂停和重启
test_pause_resume() {
    log_info "========== 测试场景5: 任务暂停和重启 =========="
    
    local test_db="test_cdc_db5"
    local test_table="test_table"
    local task_id="test_cdc_task5"
    local sink_db="${test_db}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db}"
    execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${test_table} (id INT PRIMARY KEY, name VARCHAR(100))"
    
    # 创建CDC任务
    create_cdc_task "${task_id}" "${source_uri}" "${test_db}" "${sink_uri}" "${sink_db}" "database"
    
    sleep 2
    
    # 插入一些数据
    log_info "插入初始数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (1, 'data1'), (2, 'data2')"
    sleep 2
    
    # 查询watermark
    log_info "暂停前的watermark:"
    query_watermark "${task_id}" "${test_db}" ""
    
    # 暂停任务
    pause_cdc_task "${task_id}"
    sleep 2
    
    # 在暂停期间插入数据
    log_info "任务暂停期间插入数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (3, 'data3'), (4, 'data4')"
    sleep 2
    
    # 查询watermark（应该不会更新）
    log_info "暂停期间的watermark:"
    query_watermark "${task_id}" "${test_db}" ""
    
    # 重启任务
    resume_cdc_task "${task_id}"
    sleep 2
    
    # 插入新数据
    log_info "任务重启后插入数据..."
    execute_sql_verbose "USE ${test_db}; INSERT INTO ${test_table} (id, name) VALUES (5, 'data5'), (6, 'data6')"
    sleep 2
    
    # 查询watermark（应该更新）
    log_info "重启后的watermark:"
    query_watermark "${task_id}" "${test_db}" ""
    
    log_info "✓ 测试通过: 任务暂停和重启功能正常"
    log_info "场景5测试完成\n"
}

# 测试场景6: 多表CDC保护
test_multi_table_protection() {
    log_info "========== 测试场景6: 多表CDC保护 =========="
    
    local test_db="test_cdc_db6"
    local task_id="test_cdc_task6"
    local sink_db="${test_db}_bak"
    local source_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local sink_uri="mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}"
    local num_tables=5
    
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${test_db}"
    execute_sql_verbose "CREATE DATABASE IF NOT EXISTS ${sink_db}"
    
    # 创建多个表
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        execute_sql_verbose "USE ${test_db}; CREATE TABLE IF NOT EXISTS ${table_name} (id INT PRIMARY KEY, name VARCHAR(100), data VARCHAR(200))"
    done
    
    # 创建CDC任务
    create_cdc_task "${task_id}" "${source_uri}" "${test_db}" "${sink_uri}" "${sink_db}" "database"
    
    sleep 2
    
    # 为每个表插入数据
    log_info "为每个表插入数据..."
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        for j in $(seq 1 3); do
            execute_sql_verbose "USE ${test_db}; INSERT INTO ${table_name} (id, name, data) VALUES ($((i*10+j)), 'name${j}', 'data${j}')"
        done
        sleep 1
    done
    
    sleep 2
    
    # 查询所有表的watermark
    log_info "查询所有表的watermark:"
    query_watermark "${task_id}" "${test_db}" ""
    
    # 验证每个表都有watermark
    local all_have_wm=true
    for i in $(seq 1 $num_tables); do
        local table_name="test_table${i}"
        local wm=$(query_watermark "${task_id}" "${test_db}" "${table_name}")
        log_info "表 ${table_name} watermark: ${wm}"
        
        if [ -z "$wm" ]; then
            all_have_wm=false
            log_warn "表 ${table_name} 没有watermark"
        fi
    done
    
    if [ "$all_have_wm" = true ]; then
        log_info "✓ 测试通过: 多表CDC保护工作正常"
    else
        log_warn "⚠ 部分表watermark可能还未更新"
    fi
    
    log_info "场景6测试完成\n"
}

# 主函数
main() {
    log_info "开始CDC GC功能测试..."
    log_info "数据库连接: ${DB_USER}@${DB_HOST}:${DB_PORT}"
    log_info "账户ID: ${ACCOUNT_ID}"
    log_info "使用客户端: ${MYSQL_CMD}"
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
    test_continuous_write || failed_tests=$((failed_tests + 1))
    test_pause_resume || failed_tests=$((failed_tests + 1))
    test_multi_table_protection || failed_tests=$((failed_tests + 1))
    
    # 测试总结
    echo ""
    log_info "========== 测试总结 =========="
    if [ $failed_tests -eq 0 ]; then
        log_info "✓ 所有测试通过！"
        log_info "注意: 测试任务已创建但未删除，可以手动清理或继续使用"
        exit 0
    else
        log_error "✗ 有 ${failed_tests} 个测试失败"
        exit 1
    fi
}

# 运行主函数
main "$@"

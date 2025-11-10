#!/bin/bash

# CDC GC 快速验证脚本
# 用于快速验证CDC功能是否正常工作

set -e

# 配置参数
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-6001}"
DB_USER="${DB_USER:-dump}"
DB_PASS="${DB_PASS:-111}"
ACCOUNT_ID="${ACCOUNT_ID:-0}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检测MySQL客户端
MYSQL_CMD=""
if command -v mysql &> /dev/null; then
    MYSQL_CMD="mysql"
elif command -v mo &> /dev/null; then
    MYSQL_CMD="mo"
else
    log_error "未找到mysql或mo客户端，请安装MySQL客户端"
    exit 1
fi

log_info "使用客户端: ${MYSQL_CMD}"

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

# 主函数
main() {
    log_info "========== CDC GC 快速验证 =========="
    log_info "数据库连接: ${DB_USER}@${DB_HOST}:${DB_PORT}"
    log_info "账户ID: ${ACCOUNT_ID}"
    echo ""
    
    # 1. 测试数据库连接
    log_info "1. 测试数据库连接..."
    if ! execute_sql "SELECT 1" > /dev/null 2>&1; then
        log_error "无法连接到数据库，请检查连接参数"
        exit 1
    fi
    log_info "✓ 数据库连接成功"
    
    # 2. 检查CDC表是否存在
    log_info "2. 检查mo_cdc_watermark表..."
    local table_exists=$(execute_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='mo_catalog' AND table_name='mo_cdc_watermark'" 2>/dev/null || echo "0")
    if [ "$table_exists" = "0" ]; then
        log_error "mo_cdc_watermark表不存在，请确保CDC功能已启用"
        exit 1
    fi
    log_info "✓ mo_cdc_watermark表存在"
    
    # 3. 检查表结构
    log_info "3. 检查表结构..."
    execute_sql_verbose "DESC mo_catalog.mo_cdc_watermark"
    
    # 4. 查看当前CDC记录
    log_info "4. 查看当前CDC记录..."
    local cdc_count=$(execute_sql "SELECT COUNT(*) FROM mo_catalog.mo_cdc_watermark" 2>/dev/null || echo "0")
    log_info "当前CDC记录数: ${cdc_count}"
    if [ "$cdc_count" -gt 0 ]; then
        execute_sql_verbose "SELECT account_id, task_id, db_name, table_name, watermark FROM mo_catalog.mo_cdc_watermark LIMIT 10"
    fi
    
    # 5. 检查GC配置
    log_info "5. 检查GC相关配置..."
    execute_sql_verbose "SHOW VARIABLES LIKE '%gc%'" || log_warn "无法查询GC配置"
    
    # 6. 检查checkpoint配置
    log_info "6. 检查checkpoint配置..."
    execute_sql_verbose "SHOW VARIABLES LIKE '%checkpoint%'" || log_warn "无法查询checkpoint配置"
    
    # 7. 检查flush配置
    log_info "7. 检查flush配置..."
    execute_sql_verbose "SHOW VARIABLES LIKE '%flush%'" || log_warn "无法查询flush配置"
    
    echo ""
    log_info "========== 验证完成 =========="
    log_info "如果所有检查都通过，可以运行完整的测试脚本: ./test_cdc_gc.sh"
}

main "$@"


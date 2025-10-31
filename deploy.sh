#!/bin/bash

# MatrixOne 多CN集群部署脚本
# 使用方法: ./deploy.sh [start|stop|restart|status|logs]

set -e

COMPOSE_FILE="docker-compose-multi-cn.yaml"

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker和Docker Compose
check_dependencies() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker未安装，请先安装Docker"
        exit 1
    fi

    if ! command -v docker compose &> /dev/null; then
        print_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi

    print_info "依赖检查通过"
}

# 检查配置文件
check_configs() {
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "找不到 $COMPOSE_FILE 文件"
        exit 1
    fi

    if [ ! -d "config" ]; then
        print_error "找不到 config 目录"
        exit 1
    fi

    local required_files=("config/log.toml" "config/tn.toml" "config/cn-0.toml" "config/cn-1.toml")
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "找不到配置文件: $file"
            exit 1
        fi
    done

    print_info "配置文件检查通过"
}

# 启动服务
start_services() {
    print_info "启动MatrixOne多CN集群..."
    docker compose -f "$COMPOSE_FILE" up -d
    print_info "服务启动完成"
    echo ""
    print_info "服务状态:"
    docker compose -f "$COMPOSE_FILE" ps
    echo ""
    print_info "等待服务就绪（30秒）..."
    sleep 30
    print_info "部署完成！"
    echo ""
    print_info "连接信息:"
    echo "  CN-0 MySQL: mysql -h 127.0.0.1 -P 6001 -u root -p"
    echo "  CN-1 MySQL: mysql -h 127.0.0.1 -P 6003 -u root -p"
    echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
}

# 停止服务
stop_services() {
    print_info "停止MatrixOne多CN集群..."
    docker compose -f "$COMPOSE_FILE" down
    print_info "服务已停止"
}

# 重启服务
restart_services() {
    print_info "重启MatrixOne多CN集群..."
    stop_services
    sleep 5
    start_services
}

# 查看状态
show_status() {
    print_info "服务状态:"
    docker compose -f "$COMPOSE_FILE" ps
    echo ""
    print_info "服务健康检查:"
    docker compose -f "$COMPOSE_FILE" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
}

# 查看日志
show_logs() {
    if [ -z "$2" ]; then
        print_info "查看所有服务日志（按Ctrl+C退出）..."
        docker compose -f "$COMPOSE_FILE" logs -f
    else
        print_info "查看 $2 服务日志（按Ctrl+C退出）..."
        docker compose -f "$COMPOSE_FILE" logs -f "$2"
    fi
}

# 主函数
main() {
    case "$1" in
        start)
            check_dependencies
            check_configs
            start_services
            ;;
        stop)
            check_dependencies
            stop_services
            ;;
        restart)
            check_dependencies
            check_configs
            restart_services
            ;;
        status)
            check_dependencies
            show_status
            ;;
        logs)
            check_dependencies
            show_logs "$@"
            ;;
        *)
            echo "使用方法: $0 [start|stop|restart|status|logs [service-name]]"
            echo ""
            echo "命令说明:"
            echo "  start          - 启动集群"
            echo "  stop           - 停止集群"
            echo "  restart        - 重启集群"
            echo "  status         - 查看服务状态"
            echo "  logs           - 查看所有服务日志"
            echo "  logs [service] - 查看指定服务日志（如: logs cn-0）"
            echo ""
            echo "可用服务名称:"
            echo "  logservice, tn, cn-0, cn-1, minio"
            exit 1
            ;;
    esac
}

main "$@"


#!/bin/bash

# LogService 磁盘清理脚本

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}LogService 磁盘清理工具${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}错误: Docker 未运行，请先启动 Docker${NC}"
    exit 1
fi

# 显示当前磁盘使用情况
echo -e "${YELLOW}步骤 1: 检查当前磁盘使用情况${NC}"
echo ""

echo "Docker 系统空间使用:"
docker system df

echo ""
echo "LogService 容器磁盘使用:"
if docker ps --format "{{.Names}}" | grep -q "^logservice$"; then
    docker exec logservice df -h 2>/dev/null || echo "无法获取容器内磁盘信息"
else
    echo "LogService 容器未运行"
fi

echo ""
if [ -d "mo-data/logservice-data" ]; then
    echo "LogService 数据目录大小:"
    du -sh mo-data/logservice-data/* 2>/dev/null | head -5
    echo ""
    echo "Snapshots 数量:"
    find mo-data/logservice-data -name "snapshot-*" -type d 2>/dev/null | wc -l | xargs echo
else
    echo "LogService 数据目录未映射到宿主机"
fi

echo ""
echo -e "${YELLOW}步骤 2: 选择清理选项${NC}"
echo ""
echo "1. 清理 Docker 系统空间（未使用的容器、镜像、网络）"
echo "2. 清理 LogService 旧的 Snapshots（保留最新的 5 个）"
echo "3. 清理 Docker 构建缓存"
echo "4. 执行所有清理操作"
echo "5. 退出"
echo ""

read -p "请选择 (1-5): " choice

case $choice in
    1)
        echo ""
        echo -e "${YELLOW}清理 Docker 系统空间...${NC}"
        docker system prune -f
        echo -e "${GREEN}✓ 清理完成${NC}"
        ;;
    2)
        echo ""
        echo -e "${YELLOW}清理 LogService 旧的 Snapshots...${NC}"
        if [ -d "mo-data/logservice-data" ]; then
            # 查找所有 snapshot 目录，按时间排序，删除除最新 5 个外的所有
            snapshot_dirs=$(find mo-data/logservice-data -name "snapshot-*" -type d | sort -r)
            total=$(echo "$snapshot_dirs" | wc -l | xargs)
            if [ "$total" -gt 5 ]; then
                to_delete=$(echo "$snapshot_dirs" | tail -n +6)
                echo "找到 $total 个 snapshots，将删除 $((total - 5)) 个旧的"
                echo "$to_delete" | while read dir; do
                    if [ -n "$dir" ]; then
                        echo "删除: $dir"
                        rm -rf "$dir"
                    fi
                done
                echo -e "${GREEN}✓ 清理完成${NC}"
            else
                echo "只有 $total 个 snapshots，无需清理"
            fi
        else
            echo -e "${RED}错误: LogService 数据目录未映射到宿主机${NC}"
        fi
        ;;
    3)
        echo ""
        echo -e "${YELLOW}清理 Docker 构建缓存...${NC}"
        docker builder prune -f
        echo -e "${GREEN}✓ 清理完成${NC}"
        ;;
    4)
        echo ""
        echo -e "${YELLOW}执行所有清理操作...${NC}"
        echo ""
        
        echo "1. 清理 Docker 系统空间..."
        docker system prune -f
        echo ""
        
        echo "2. 清理 Docker 构建缓存..."
        docker builder prune -f
        echo ""
        
        if [ -d "mo-data/logservice-data" ]; then
            echo "3. 清理 LogService 旧的 Snapshots..."
            snapshot_dirs=$(find mo-data/logservice-data -name "snapshot-*" -type d | sort -r)
            total=$(echo "$snapshot_dirs" | wc -l | xargs)
            if [ "$total" -gt 5 ]; then
                to_delete=$(echo "$snapshot_dirs" | tail -n +6)
                echo "$to_delete" | while read dir; do
                    if [ -n "$dir" ]; then
                        rm -rf "$dir"
                    fi
                done
                echo "删除了 $((total - 5)) 个旧的 snapshots"
            fi
        fi
        
        echo ""
        echo -e "${GREEN}✓ 所有清理操作完成${NC}"
        ;;
    5)
        echo "退出"
        exit 0
        ;;
    *)
        echo -e "${RED}无效的选择${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${YELLOW}步骤 3: 清理后的磁盘使用情况${NC}"
echo ""

echo "Docker 系统空间使用:"
docker system df

echo ""
if [ -d "mo-data/logservice-data" ]; then
    echo "LogService 数据目录大小:"
    du -sh mo-data/logservice-data/* 2>/dev/null | head -5
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}清理完成！${NC}"
echo -e "${GREEN}========================================${NC}"











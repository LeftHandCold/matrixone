#!/bin/bash

# Docker 磁盘使用情况检查脚本（Linux）

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Docker 磁盘使用情况检查（Linux）${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}错误: Docker 未运行，请先启动 Docker${NC}"
    exit 1
fi

# Docker 存储信息
echo -e "${YELLOW}=== Docker 存储信息 ===${NC}"
echo ""
docker info | grep -E "Storage Driver|Docker Root Dir" || echo "无法获取 Docker 信息"

echo ""
echo -e "${YELLOW}=== 宿主机磁盘使用情况 ===${NC}"
echo ""
df -h

echo ""
echo -e "${YELLOW}=== Docker 数据目录所在分区 ===${NC}"
echo ""
DOCKER_ROOT=$(docker info 2>/dev/null | grep "Docker Root Dir" | awk '{print $4}' || echo "/var/lib/docker")
df -h "$DOCKER_ROOT"

echo ""
echo -e "${YELLOW}=== Docker 数据目录大小（Top 10）===${NC}"
echo ""
if [ -d "$DOCKER_ROOT" ]; then
    sudo du -sh "$DOCKER_ROOT"/* 2>/dev/null | sort -h | tail -10 || echo "无法访问 Docker 数据目录（需要 sudo 权限）"
else
    echo "Docker 数据目录不存在: $DOCKER_ROOT"
fi

echo ""
echo -e "${YELLOW}=== Docker 系统空间使用 ===${NC}"
echo ""
docker system df

echo ""
echo -e "${YELLOW}=== Docker 容器日志大小（Top 10）===${NC}"
echo ""
if [ -d "$DOCKER_ROOT/containers" ]; then
    sudo du -sh "$DOCKER_ROOT/containers"/*/ 2>/dev/null | sort -h | tail -10 || echo "无法访问容器日志目录（需要 sudo 权限）"
fi

echo ""
echo -e "${YELLOW}=== LogService 数据大小（如果存在）===${NC}"
echo ""
if [ -d "mo-data/logservice-data" ]; then
    echo "LogService 数据目录:"
    du -sh mo-data/logservice-data/* 2>/dev/null | head -5
    echo ""
    echo "Snapshots 数量:"
    find mo-data/logservice-data -name "snapshot-*" -type d 2>/dev/null | wc -l | xargs echo
else
    echo "LogService 数据目录未映射到宿主机"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}检查完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "建议操作："
echo "1. 如果 Docker 数据目录空间不足，考虑迁移到更大的分区"
echo "2. 运行 'docker system prune -a' 清理未使用的资源"
echo "3. 配置 Docker 日志轮转（编辑 /etc/docker/daemon.json）"
echo "4. 清理 LogService 旧的 snapshots（如果存在）"
echo ""











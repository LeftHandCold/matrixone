#!/bin/bash

# 更新 MatrixOne 集群脚本
# 用于在更新代码后重新构建镜像并更新运行中的集群

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}MatrixOne 集群更新脚本${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 检查是否在项目根目录
if [ ! -f "go.mod" ] || [ ! -f "Makefile" ]; then
    echo -e "${RED}错误: 请在 MatrixOne 项目根目录运行此脚本${NC}"
    exit 1
fi

# 获取 GOPROXY（默认使用国内代理）
GOPROXY=${GOPROXY:-"https://goproxy.cn,direct"}

echo -e "${YELLOW}步骤 1/3: 重新构建 Docker 镜像${NC}"
echo "GOPROXY: $GOPROXY"
echo ""

docker build -f optools/images/Dockerfile \
    --build-arg GOPROXY="$GOPROXY" \
    -t matrixorigin/matrixone:latest .

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 镜像构建失败${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ 镜像构建成功${NC}"
echo ""

# 停止并删除集群（保留数据卷，使用新镜像）
echo -e "${YELLOW}步骤 2/3: 停止现有集群并删除容器${NC}"
cd etc/launch-tae-compose

echo "停止并删除容器（保留数据卷）..."
echo "注意: 使用 'down' 而不是 'restart'，确保使用新镜像"
docker compose --profile launch-multi-cn down

echo ""
echo -e "${GREEN}✓ 集群已停止并删除${NC}"
echo ""

# 重新启动集群
echo -e "${YELLOW}步骤 3/3: 重新启动集群（使用新镜像）${NC}"
docker compose --profile launch-multi-cn up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 集群启动失败${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ 集群已启动${NC}"
echo ""

# 等待服务启动
echo -e "${YELLOW}等待服务启动（15秒）...${NC}"
sleep 15

# 显示服务状态
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}集群状态${NC}"
echo -e "${GREEN}========================================${NC}"
docker compose --profile launch-multi-cn ps

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}更新完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "查看日志:"
echo "  docker compose --profile launch-multi-cn logs -f"
echo ""
echo "查看特定服务日志:"
echo "  docker compose --profile launch-multi-cn logs -f cn-0"
echo "  docker compose --profile launch-multi-cn logs -f cn-1"
echo "  docker compose --profile launch-multi-cn logs -f logservice"
echo ""


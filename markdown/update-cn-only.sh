#!/bin/bash

# 只更新 CN 服务脚本
# 用于在更新代码后只更新 cn-0 和 cn-1，而不影响其他服务（tn、logservice、minio 等）

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}只更新 CN 服务脚本${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查是否在项目根目录
if [ ! -f "go.mod" ] || [ ! -f "Makefile" ]; then
    echo -e "${RED}错误: 请在 MatrixOne 项目根目录运行此脚本${NC}"
    exit 1
fi

# 检查是否需要构建镜像
BUILD_IMAGE=${1:-"yes"}
if [ "$BUILD_IMAGE" = "no" ] || [ "$BUILD_IMAGE" = "skip" ]; then
    echo -e "${YELLOW}跳过镜像构建（使用现有镜像）${NC}"
    BUILD_IMAGE="no"
else
    # 获取 GOPROXY（默认使用国内代理）
    GOPROXY=${GOPROXY:-"https://goproxy.cn,direct"}
    
    echo -e "${YELLOW}步骤 1/4: 重新构建 Docker 镜像${NC}"
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
fi

# 切换到 compose 目录
cd etc/launch-tae-compose

# 检查服务是否运行
echo -e "${YELLOW}步骤 $([ "$BUILD_IMAGE" = "no" ] && echo "1" || echo "2")/4: 检查服务状态${NC}"
if ! docker compose --profile launch-multi-cn ps cn-0 cn-1 | grep -q "Up"; then
    echo -e "${YELLOW}警告: cn-0 或 cn-1 未运行，将启动它们${NC}"
fi

# 停止 CN 服务（不停止依赖服务）
echo ""
echo -e "${YELLOW}步骤 $([ "$BUILD_IMAGE" = "no" ] && echo "2" || echo "3")/4: 停止 CN 服务${NC}"
echo "停止 cn-0 和 cn-1（不影响其他服务）..."
docker compose --profile launch-multi-cn stop cn-0 cn-1

echo ""
echo -e "${GREEN}✓ CN 服务已停止${NC}"
echo ""

# 删除 CN 容器（强制重新创建）
echo -e "${YELLOW}步骤 $([ "$BUILD_IMAGE" = "no" ] && echo "3" || echo "4")/4: 删除 CN 容器${NC}"
echo "删除 cn-0 和 cn-1 容器（使用新镜像重新创建）..."
docker compose --profile launch-multi-cn rm -f cn-0 cn-1

echo ""
echo -e "${GREEN}✓ CN 容器已删除${NC}"
echo ""

# 重新创建并启动 CN 服务（使用新镜像）
echo -e "${YELLOW}步骤 $([ "$BUILD_IMAGE" = "no" ] && echo "4" || echo "5")/4: 重新创建并启动 CN 服务${NC}"
echo "使用新镜像重新创建 cn-0 和 cn-1..."
echo "注意: 使用 --no-deps 确保不重新创建依赖服务（tn、logservice 等）"
docker compose --profile launch-multi-cn up -d --no-deps --force-recreate cn-0 cn-1

if [ $? -ne 0 ]; then
    echo -e "${RED}错误: CN 服务启动失败${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ CN 服务已启动${NC}"
echo ""

# 等待服务启动
echo -e "${YELLOW}等待 CN 服务启动（10秒）...${NC}"
sleep 10

# 显示服务状态
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CN 服务状态${NC}"
echo -e "${GREEN}========================================${NC}"
docker compose --profile launch-multi-cn ps cn-0 cn-1

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}更新完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}其他服务状态（未更新）:${NC}"
docker compose --profile launch-multi-cn ps tn logservice minio

echo ""
echo "查看 CN 日志:"
echo "  docker compose --profile launch-multi-cn logs -f cn-0"
echo "  docker compose --profile launch-multi-cn logs -f cn-1"
echo ""
echo "查看所有服务日志:"
echo "  docker compose --profile launch-multi-cn logs -f"
echo ""










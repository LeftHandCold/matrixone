#!/bin/bash
# 断开CN容器的网络连接，模拟网络错误
# 用法: ./disconnect-cn-network.sh cn-0 [network-name]

set -e

CN_NAME=${1:-cn-1}
NETWORK_NAME=${2:-}

if [ -z "$CN_NAME" ]; then
    echo "用法: $0 <cn-container-name> [network-name]"
    echo "示例: $0 cn-1"
    echo "     $0 cn-0 launch-tae-compose_default"
    exit 1
fi

# 如果没有指定网络名，自动查找容器连接的网络
if [ -z "$NETWORK_NAME" ]; then
    echo "查找容器 $CN_NAME 连接的网络..."
    NETWORK_NAME=$(sudo docker inspect $CN_NAME --format '{{range $key, $value := .NetworkSettings.Networks}}{{$key}}{{end}}' | head -1)
    if [ -z "$NETWORK_NAME" ]; then
        echo "错误: 无法找到容器 $CN_NAME 的网络，请手动指定网络名"
        echo "可用网络:"
        sudo docker network ls | grep -v "^NETWORK"
        exit 1
    fi
fi

echo "断开容器 $CN_NAME 与网络 $NETWORK_NAME 的连接..."
sudo docker network disconnect $NETWORK_NAME $CN_NAME || {
    echo "错误: 断开连接失败"
    exit 1
}

echo "✓ 容器 $CN_NAME 已断开与网络 $NETWORK_NAME 的连接"
echo ""
echo "查看容器状态:"
sudo docker ps --filter "name=$CN_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Networks}}"
echo ""
echo "要恢复网络连接，运行:"
echo "  sudo docker network connect $NETWORK_NAME $CN_NAME"


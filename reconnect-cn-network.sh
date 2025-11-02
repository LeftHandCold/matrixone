#!/bin/bash
# 恢复CN容器的网络连接
# 用法: ./reconnect-cn-network.sh cn-0 [network-name]

set -e

CN_NAME=${1:-cn-1}
NETWORK_NAME=${2:-}

if [ -z "$CN_NAME" ]; then
    echo "用法: $0 <cn-container-name> [network-name]"
    echo "示例: $0 cn-1"
    exit 1
fi

# 如果没有指定网络名，自动查找容器之前连接的网络
if [ -z "$NETWORK_NAME" ]; then
    echo "查找容器 $CN_NAME 应该连接的网络..."
    # 查找docker compose的网络（通常是项目名_default）
    COMPOSE_PROJECT=$(sudo docker inspect $CN_NAME --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null || echo "")
    
    if [ -z "$COMPOSE_PROJECT" ]; then
        # 尝试从其他CN容器获取网络名
        OTHER_CN=$(sudo docker ps --filter "name=cn-" --format "{{.Names}}" | grep -v "$CN_NAME" | head -1)
        if [ -n "$OTHER_CN" ]; then
            NETWORK_NAME=$(sudo docker inspect $OTHER_CN --format '{{range $key, $value := .NetworkSettings.Networks}}{{$key}}{{end}}' | head -1)
        fi
    else
        NETWORK_NAME="${COMPOSE_PROJECT}_default"
    fi
    
    if [ -z "$NETWORK_NAME" ]; then
        echo "错误: 无法确定网络名，请手动指定"
        echo "可用网络:"
        sudo docker network ls | grep -v "^NETWORK"
        exit 1
    fi
fi

echo "恢复容器 $CN_NAME 与网络 $NETWORK_NAME 的连接..."
sudo docker network connect $NETWORK_NAME $CN_NAME || {
    echo "错误: 恢复连接失败"
    exit 1
}

echo "✓ 容器 $CN_NAME 已恢复与网络 $NETWORK_NAME 的连接"
echo ""
echo "验证连接:"
sudo docker ps --filter "name=$CN_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Networks}}"


# 仅更新 CN 容器使用最新镜像

## 方法1：使用 docker compose up --force-recreate（推荐）

这是最简单的方法，只重新创建 CN 容器，不影响其他服务（TN、LogService、MinIO 等）。

```bash
cd etc/launch-tae-compose

# 确保最新镜像已构建
docker build -f ../../optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest \
  ../../

# 强制重新创建 CN 容器（使用最新镜像）
docker compose up -d --force-recreate --no-deps cn-0 cn-1
```

**参数说明**：
- `--force-recreate`: 强制重新创建容器（即使配置未改变）
- `--no-deps`: 不重新创建依赖的服务（TN、LogService 等）
- `cn-0 cn-1`: 只更新这两个服务

**优点**：
- 简单快速
- 不影响其他服务
- 保留所有数据卷和网络配置

---

## 方法2：停止 → 删除 → 启动

如果需要更精确的控制：

```bash
cd etc/launch-tae-compose

# 1. 停止 CN 容器
docker compose stop cn-0 cn-1

# 2. 删除 CN 容器（保留数据卷）
docker compose rm -f cn-0 cn-1

# 3. 确保最新镜像已构建
docker build -f ../../optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest \
  ../../

# 4. 重新启动 CN 容器（使用最新镜像）
docker compose up -d --no-deps cn-0 cn-1
```

**优点**：
- 更明确的操作步骤
- 可以验证镜像是否已更新

---

## 方法3：使用 docker stop/rm/run（手动控制）

如果需要完全手动控制：

```bash
# 1. 停止并删除 CN 容器
docker stop cn-0 cn-1
docker rm cn-0 cn-1

# 2. 确保最新镜像已构建
docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest \
  .

# 3. 使用 docker compose 重新创建（会自动使用最新镜像）
cd etc/launch-tae-compose
docker compose up -d --no-deps cn-0 cn-1
```

---

## 验证更新是否成功

### 1. 检查容器使用的镜像

```bash
# 查看 CN 容器使用的镜像 ID
docker inspect cn-0 --format='{{.Image}}'
docker inspect cn-1 --format='{{.Image}}'

# 查看最新镜像的 ID
docker images matrixorigin/matrixone:latest --format='{{.ID}}'

# 对比两者是否一致
```

### 2. 检查容器状态

```bash
docker compose ps cn-0 cn-1
```

### 3. 查看日志确认新代码已加载

```bash
# 查看 CN0 日志（应该看到新的日志输出）
docker compose logs -f cn-0 | head -50

# 查看 CN1 日志
docker compose logs -f cn-1 | head -50
```

### 4. 测试新功能

如果代码中有新的日志或功能，可以通过查询验证：

```bash
# 连接到 CN0
mysql -h 127.0.0.1 -P 6001 -u root -p

# 执行测试查询，查看日志中是否有新的输出
```

---

## 注意事项

### 1. 数据不会丢失

- 所有数据卷（MinIO、LogService）保持不变
- 容器删除不会影响数据卷
- 重新创建容器后，数据卷会自动重新挂载

### 2. 服务短暂中断

- CN 容器重启时，连接到该 CN 的查询会中断
- 建议在低峰期执行更新
- 如果使用连接池，会自动重连到其他 CN

### 3. 依赖关系

- CN 依赖 TN，但使用 `--no-deps` 不会重启 TN
- 如果 TN 也在运行，CN 会自动重连
- 如果 TN 未运行，CN 会等待 TN 启动

### 4. 镜像标签

- 确保使用相同的镜像标签（`matrixorigin/matrixone:latest`）
- 如果使用不同的标签，需要修改 `compose.yaml` 中的 `image` 字段

---

## 快速脚本

创建一个脚本 `update-cn-only.sh`：

```bash
#!/bin/bash
set -e

echo "Building latest image..."
docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest \
  .

echo "Updating CN containers..."
cd etc/launch-tae-compose
docker compose up -d --force-recreate --no-deps cn-0 cn-1

echo "Waiting for CNs to start..."
sleep 5

echo "Checking CN status..."
docker compose ps cn-0 cn-1

echo "Done! CN containers updated."
```

使用方法：
```bash
chmod +x update-cn-only.sh
./update-cn-only.sh
```

---

## 总结

**推荐使用方法1**（`docker compose up -d --force-recreate --no-deps cn-0 cn-1`），因为：
- 最简单
- 一条命令完成
- 不影响其他服务
- 保留所有配置和数据

**执行前确保**：
1. 最新镜像已构建完成
2. 镜像标签与 `compose.yaml` 中的一致
3. 在合适的时机执行（避免影响正在运行的查询）


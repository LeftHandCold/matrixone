# 只更新 CN 服务指南

## 概述

当你更新了代码并构建了新镜像，但只想更新 `cn-0` 和 `cn-1` 两个 CN 服务，而不影响其他服务（tn、logservice、minio 等）时，可以使用本指南。

## 快速使用

### 方法 1: 使用脚本（推荐）

```bash
# 构建新镜像并更新 CN 服务
./update-cn-only.sh

# 如果镜像已构建，跳过构建步骤
./update-cn-only.sh skip
```

### 方法 2: 手动操作

```bash
# 1. 构建新镜像（如果还没构建）
docker build -f optools/images/Dockerfile \
    --build-arg GOPROXY="https://goproxy.cn,direct" \
    -t matrixorigin/matrixone:latest .

# 2. 切换到 compose 目录
cd etc/launch-tae-compose

# 3. 停止 CN 服务
docker compose --profile launch-multi-cn stop cn-0 cn-1

# 4. 删除 CN 容器
docker compose --profile launch-multi-cn rm -f cn-0 cn-1

# 5. 重新创建并启动 CN 服务（使用新镜像）
docker compose --profile launch-multi-cn up -d --no-deps --force-recreate cn-0 cn-1

# 6. 检查状态
docker compose --profile launch-multi-cn ps cn-0 cn-1
```

## 详细说明

### 为什么使用 `--no-deps`？

`--no-deps` 参数确保只更新指定的服务（cn-0 和 cn-1），而不会重新创建它们的依赖服务（tn、logservice 等）。这样可以：

- ✅ 保持其他服务运行，避免不必要的重启
- ✅ 减少服务中断时间
- ✅ 避免影响正在运行的查询和事务

### 为什么使用 `--force-recreate`？

`--force-recreate` 强制重新创建容器，即使配置没有变化。这样可以确保：

- ✅ 使用最新的镜像
- ✅ 清理旧的容器状态
- ✅ 应用最新的代码更改

### 服务依赖关系

```
cn-0, cn-1
  └── depends_on: tn
        └── depends_on: logservice
              └── depends_on: createbuckets
                    └── depends_on: minio
```

使用 `--no-deps` 时，只更新 cn-0 和 cn-1，不会影响它们的依赖服务。

## 验证更新

### 检查服务状态

```bash
cd etc/launch-tae-compose

# 查看 CN 服务状态
docker compose --profile launch-multi-cn ps cn-0 cn-1

# 查看所有服务状态
docker compose --profile launch-multi-cn ps
```

### 查看日志

```bash
# 查看 cn-0 日志
docker compose --profile launch-multi-cn logs -f cn-0

# 查看 cn-1 日志
docker compose --profile launch-multi-cn logs -f cn-1

# 查看所有服务日志
docker compose --profile launch-multi-cn logs -f
```

### 验证镜像版本

```bash
# 查看 CN 容器使用的镜像
docker inspect cn-0 | grep -A 5 "Image"
docker inspect cn-1 | grep -A 5 "Image"

# 应该显示: matrixorigin/matrixone:latest
```

## 常见问题

### Q: 更新后 CN 服务无法启动？

**A:** 检查日志：

```bash
docker compose --profile launch-multi-cn logs cn-0
docker compose --profile launch-multi-cn logs cn-1
```

可能的原因：
- 新代码有兼容性问题
- 配置文件不匹配
- 依赖服务（tn、logservice）未运行

### Q: 更新后查询失败？

**A:** 检查服务连接：

```bash
# 检查 CN 是否能连接到 TN
docker compose --profile launch-multi-cn logs cn-0 | grep -i "tn\|error"

# 检查 CN 是否能连接到 LogService
docker compose --profile launch-multi-cn logs cn-0 | grep -i "logservice\|error"
```

### Q: 如何回滚到旧版本？

**A:** 如果新版本有问题，可以回滚：

```bash
# 1. 停止 CN 服务
docker compose --profile launch-multi-cn stop cn-0 cn-1

# 2. 删除 CN 容器
docker compose --profile launch-multi-cn rm -f cn-0 cn-1

# 3. 使用旧镜像重新创建（假设旧镜像标签为 old-version）
docker tag matrixorigin/matrixone:old-version matrixorigin/matrixone:latest
docker compose --profile launch-multi-cn up -d --no-deps --force-recreate cn-0 cn-1
```

### Q: 更新后其他服务（tn、logservice）需要重启吗？

**A:** 通常不需要。但如果新代码有协议或接口变更，可能需要：

```bash
# 更新所有服务（包括依赖服务）
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn down
docker compose --profile launch-multi-cn up -d
```

## 最佳实践

1. **先测试再更新生产环境**
   - 在测试环境验证新代码
   - 确认新版本正常工作后再更新生产环境

2. **更新前备份**
   - 备份重要数据
   - 记录当前镜像版本

3. **逐步更新**
   - 先更新一个 CN（如 cn-0）
   - 验证正常后再更新另一个 CN（cn-1）

4. **监控更新过程**
   - 查看日志确认服务正常启动
   - 检查服务状态和连接

5. **保留回滚方案**
   - 保留旧镜像
   - 准备回滚脚本

## 与完整更新的区别

| 操作 | 完整更新 (`update-cluster.sh`) | 只更新 CN (`update-cn-only.sh`) |
|------|-------------------------------|--------------------------------|
| 更新服务 | 所有服务（cn-0, cn-1, tn, logservice, minio） | 只更新 cn-0 和 cn-1 |
| 服务中断 | 所有服务都会重启 | 只中断 CN 服务 |
| 适用场景 | 重大更新、协议变更 | 日常代码更新、CN 特定修复 |
| 风险 | 较高（影响所有服务） | 较低（只影响 CN） |

## 相关脚本

- `update-cluster.sh`: 更新整个集群（所有服务）
- `update-cn-only.sh`: 只更新 CN 服务
- `check-docker-disk.sh`: 检查 Docker 磁盘使用情况










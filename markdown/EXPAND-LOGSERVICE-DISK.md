# LogService 磁盘扩容指南

## 问题说明

LogService 的根目录空间满了，需要扩容。有几种解决方案：

## 方案 1: 清理 Docker 系统空间（快速，推荐先尝试）

### 步骤 1: 检查磁盘使用情况

```bash
# 检查 Docker 系统空间使用
docker system df

# 检查 logservice 容器内的磁盘使用
docker exec logservice df -h

# 检查 logservice 数据目录大小
du -sh mo-data/logservice-data/*
```

### 步骤 2: 清理未使用的 Docker 资源

```bash
# 清理未使用的容器、网络、镜像（悬空镜像）
docker system prune

# 清理未使用的卷（谨慎使用，会删除未使用的卷）
docker volume prune

# 清理构建缓存
docker builder prune

# 一键清理所有未使用的资源（包括未使用的镜像）
docker system prune -a
```

**注意**: `docker system prune -a` 会删除所有未使用的镜像，包括可能正在使用的旧版本镜像。

### 步骤 3: 清理 Docker 日志

```bash
# 查看 Docker 日志大小
sudo du -sh /var/lib/docker/containers/*/

# 清理所有容器的日志（保留最近 100MB）
sudo find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 100M {} \;
```

## 方案 2: 清理 LogService 旧数据（如果数据已映射到宿主机）

### 步骤 1: 检查 LogService 数据目录

```bash
# 查看数据目录结构
ls -lh mo-data/logservice-data/*/

# 查看各子目录大小
du -sh mo-data/logservice-data/*/*/
```

### 步骤 2: 清理旧的 Snapshots

LogService 使用 Dragonboat，会生成很多 snapshots。可以清理旧的 snapshots：

```bash
# 进入 logservice 容器
docker exec -it logservice bash

# 查看 snapshots 目录
ls -lh /mo-data/logservice-data/*/shenjiangweis-MacBook-Pro-2.local/*/exported-snapshot/

# 删除旧的 snapshots（保留最新的 5 个）
# 注意：这需要根据实际路径调整
find /mo-data/logservice-data -name "snapshot-*" -type d | sort -r | tail -n +6 | xargs rm -rf
```

### 步骤 3: 配置 LogService 自动清理 Snapshots

修改 `etc/launch-tae-compose/config/log.toml`，添加或修改：

```toml
[logservice]
max-exported-snapshot = 5  # 减少保留的 snapshot 数量（默认是 20）
```

然后重启 logservice：

```bash
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn restart logservice
```

## 方案 3: 将 LogService 数据映射到宿主机更大的磁盘（推荐长期方案）

### 步骤 1: 检查当前映射

查看 `etc/launch-tae-compose/compose.yaml`，确认 logservice 的 volumes 映射：

```yaml
logservice:
  volumes:
    - ../../mo-data/logservice-data:/mo-data/logservice-data
```

### 步骤 2: 迁移数据到新位置（如果需要）

如果宿主机有更大的磁盘，可以：

```bash
# 1. 停止 logservice
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn stop logservice

# 2. 备份数据
cp -r ../../mo-data/logservice-data ../../mo-data/logservice-data.backup

# 3. 将数据移动到新位置（例如更大的磁盘）
# 假设新位置是 /data/matrixone/logservice-data
sudo mkdir -p /data/matrixone
sudo mv ../../mo-data/logservice-data /data/matrixone/
sudo chown -R $USER:$USER /data/matrixone/logservice-data

# 4. 修改 compose.yaml，更新映射路径
# 将 ../../mo-data/logservice-data 改为 /data/matrixone/logservice-data

# 5. 重新启动
docker compose --profile launch-multi-cn up -d logservice
```

### 步骤 3: 修改 compose.yaml

```yaml
logservice:
  volumes:
    - ../../etc/launch-tae-compose/config:/config
    - ../../test:/test
    - ../../docker-compose-log:/log
    - /data/matrixone/logservice-data:/mo-data/logservice-data  # 改为新路径
```

## 方案 4: 扩容 Docker 的存储空间（macOS）

### 方法 A: 通过 Docker Desktop 设置

1. 打开 Docker Desktop
2. 进入 Settings → Resources → Advanced
3. 增加 Disk image size（例如从 60GB 增加到 120GB）
4. 点击 Apply & Restart

### 方法 B: 清理 Docker Desktop 的磁盘镜像

```bash
# macOS 上，Docker Desktop 使用虚拟磁盘
# 可以通过 Docker Desktop 的界面清理，或者：

# 1. 停止 Docker Desktop
# 2. 在 Docker Desktop 设置中增加磁盘大小
# 3. 重启 Docker Desktop
```

## 方案 5: 使用 Docker 卷扩容（Linux）

如果使用 Docker 的 devicemapper 或 overlay2 存储驱动：

```bash
# 检查 Docker 存储驱动
docker info | grep "Storage Driver"

# 如果是 devicemapper，需要调整 loop-lvm 的大小
# 如果是 overlay2，通常不需要特殊配置，使用宿主机文件系统
```

## 快速诊断脚本

创建一个脚本来诊断问题：

```bash
#!/bin/bash

echo "=== Docker 系统空间使用 ==="
docker system df

echo ""
echo "=== LogService 容器磁盘使用 ==="
docker exec logservice df -h 2>/dev/null || echo "LogService 容器未运行"

echo ""
echo "=== LogService 数据目录大小 ==="
if [ -d "mo-data/logservice-data" ]; then
    du -sh mo-data/logservice-data/*
    echo ""
    echo "=== Snapshots 数量 ==="
    find mo-data/logservice-data -name "snapshot-*" -type d | wc -l
else
    echo "LogService 数据目录未映射到宿主机"
fi

echo ""
echo "=== 宿主机磁盘使用 ==="
df -h
```

## 推荐操作流程

### 立即解决（快速）

```bash
# 1. 清理 Docker 系统空间
docker system prune -a

# 2. 检查是否解决
docker exec logservice df -h
```

### 长期解决（推荐）

```bash
# 1. 确保 logservice 数据已映射到宿主机（已完成）
# 2. 配置减少 snapshot 数量
# 编辑 etc/launch-tae-compose/config/log.toml
# 添加: max-exported-snapshot = 5

# 3. 重启 logservice
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn restart logservice

# 4. 定期清理旧的 snapshots（可选，通过 cron 任务）
```

## 预防措施

1. **定期清理**: 设置定期清理任务
   ```bash
   # 添加到 crontab
   0 2 * * 0 docker system prune -f
   ```

2. **监控磁盘使用**: 定期检查磁盘使用情况
   ```bash
   # 每周检查一次
   docker system df
   du -sh mo-data/logservice-data
   ```

3. **配置限制**: 在 logservice 配置中限制 snapshot 数量
   ```toml
   [logservice]
   max-exported-snapshot = 5  # 根据实际需求调整
   ```

## 注意事项

⚠️ **重要**: 
- 清理 snapshots 前，确保集群运行正常
- 不要删除正在使用的数据
- 建议先备份重要数据
- 清理 Docker 系统空间时，注意不要删除正在使用的资源

## 验证

清理后验证：

```bash
# 1. 检查磁盘空间
docker exec logservice df -h

# 2. 检查 logservice 是否正常运行
docker compose --profile launch-multi-cn ps logservice

# 3. 检查日志
docker compose --profile launch-multi-cn logs logservice | tail -20
```











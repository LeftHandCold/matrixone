# Linux 下扩容 Docker 存储空间

## 重要说明

在 Linux 下，Docker **不使用** "Disk image size" 的概念（这是 macOS/Windows Docker Desktop 的特性）。Linux 下的 Docker 直接使用宿主机的文件系统，存储空间受限于：

1. **宿主机磁盘空间**：Docker 数据存储在 `/var/lib/docker`
2. **Docker 存储驱动**：不同的存储驱动有不同的管理方式

## 方案 1: 检查当前存储使用情况（首先执行）

### 步骤 1: 检查 Docker 存储驱动

```bash
# 查看 Docker 存储驱动
docker info | grep "Storage Driver"

# 查看 Docker 根目录
docker info | grep "Docker Root Dir"
```

### 步骤 2: 检查磁盘使用情况

```bash
# 查看宿主机磁盘使用
df -h

# 查看 Docker 数据目录大小
sudo du -sh /var/lib/docker/*

# 查看 Docker 系统空间使用
docker system df
```

### 步骤 3: 检查 Docker 数据目录所在分区

```bash
# 查看 /var/lib/docker 所在分区
df -h /var/lib/docker
```

## 方案 2: 扩容宿主机磁盘（如果磁盘空间不足）

### 方法 A: 使用 LVM 扩容（如果使用 LVM）

```bash
# 1. 检查 LVM 卷组
sudo vgdisplay

# 2. 检查逻辑卷
sudo lvdisplay

# 3. 扩展逻辑卷（例如扩展 50GB）
sudo lvextend -L +50G /dev/your-vg/your-lv

# 4. 扩展文件系统（ext4）
sudo resize2fs /dev/your-vg/your-lv

# 5. 验证
df -h /var/lib/docker
```

### 方法 B: 添加新磁盘并挂载

```bash
# 1. 查看可用磁盘
lsblk

# 2. 格式化新磁盘（例如 /dev/sdb）
sudo fdisk /dev/sdb
# 创建新分区，类型为 83 (Linux)

# 3. 格式化分区
sudo mkfs.ext4 /dev/sdb1

# 4. 挂载到新位置
sudo mkdir -p /data/docker
sudo mount /dev/sdb1 /data/docker

# 5. 迁移 Docker 数据（需要停止 Docker）
sudo systemctl stop docker
sudo mv /var/lib/docker/* /data/docker/
sudo umount /data/docker
sudo mount /dev/sdb1 /var/lib/docker

# 6. 添加到 /etc/fstab 实现自动挂载
echo "/dev/sdb1 /var/lib/docker ext4 defaults 0 2" | sudo tee -a /etc/fstab

# 7. 启动 Docker
sudo systemctl start docker
```

### 方法 C: 使用符号链接指向更大的磁盘

```bash
# 1. 停止 Docker
sudo systemctl stop docker

# 2. 备份现有数据
sudo mv /var/lib/docker /var/lib/docker.backup

# 3. 在新位置创建目录（例如更大的磁盘 /data）
sudo mkdir -p /data/docker

# 4. 移动数据到新位置
sudo mv /var/lib/docker.backup/* /data/docker/

# 5. 创建符号链接
sudo ln -s /data/docker /var/lib/docker

# 6. 启动 Docker
sudo systemctl start docker
```

## 方案 3: 清理 Docker 空间（快速释放空间）

### 清理未使用的资源

```bash
# 清理未使用的容器、网络、镜像
docker system prune -a

# 清理未使用的卷
docker volume prune

# 清理构建缓存
docker builder prune -a

# 清理所有未使用的资源（包括悬空镜像）
docker system prune -a --volumes
```

### 清理 Docker 日志

```bash
# 查看日志大小
sudo du -sh /var/lib/docker/containers/*/

# 清理所有容器的日志（保留最近 100MB）
sudo find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 100M {} \;

# 或者配置 Docker 日志轮转（推荐）
```

### 配置 Docker 日志轮转

创建或编辑 `/etc/docker/daemon.json`：

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```

然后重启 Docker：

```bash
sudo systemctl restart docker
```

## 方案 4: 针对不同存储驱动的扩容

### Overlay2（默认，推荐）

Overlay2 直接使用宿主机文件系统，无需特殊配置：

```bash
# 只需确保 /var/lib/docker 所在分区有足够空间
df -h /var/lib/docker

# 如果空间不足，扩容该分区或迁移到更大的分区
```

### Devicemapper（旧版本，不推荐）

如果使用 devicemapper，需要调整 thin pool 大小：

```bash
# 1. 查看当前配置
docker info | grep -A 10 "Storage Driver"

# 2. 停止 Docker
sudo systemctl stop docker

# 3. 扩展 thin pool（需要 LVM）
sudo lvextend -L +50G /dev/docker/thinpool

# 4. 启动 Docker
sudo systemctl start docker
```

**注意**: 建议迁移到 overlay2，而不是扩容 devicemapper。

## 方案 5: 迁移 Docker 数据目录到更大的分区

### 完整迁移步骤

```bash
# 1. 停止 Docker
sudo systemctl stop docker

# 2. 备份 Docker 数据
sudo cp -a /var/lib/docker /var/lib/docker.backup

# 3. 创建新目录（在更大的分区上，例如 /data）
sudo mkdir -p /data/docker

# 4. 移动数据
sudo mv /var/lib/docker/* /data/docker/

# 5. 修改 Docker 配置
sudo mkdir -p /etc/docker
echo '{"data-root": "/data/docker"}' | sudo tee /etc/docker/daemon.json

# 6. 启动 Docker
sudo systemctl start docker

# 7. 验证
docker info | grep "Docker Root Dir"
# 应该显示: Docker Root Dir: /data/docker

# 8. 如果一切正常，删除备份
sudo rm -rf /var/lib/docker.backup
```

## 方案 6: 针对 LogService 的特定优化

### 清理 LogService 数据

```bash
# 如果 LogService 数据已映射到宿主机
cd /path/to/matrixone

# 查看 LogService 数据大小
du -sh mo-data/logservice-data/*

# 清理旧的 snapshots（保留最新的 5 个）
find mo-data/logservice-data -name "snapshot-*" -type d | sort -r | tail -n +6 | xargs rm -rf
```

### 配置 LogService 减少数据生成

编辑 `etc/launch-tae-compose/config/log.toml`：

```toml
[logservice]
max-exported-snapshot = 5  # 减少保留的 snapshot 数量
```

## 快速诊断脚本

创建脚本 `check-docker-disk.sh`：

```bash
#!/bin/bash

echo "=== Docker 存储信息 ==="
docker info | grep -E "Storage Driver|Docker Root Dir"

echo ""
echo "=== 宿主机磁盘使用 ==="
df -h

echo ""
echo "=== Docker 数据目录大小 ==="
sudo du -sh /var/lib/docker/* 2>/dev/null | sort -h | tail -10

echo ""
echo "=== Docker 系统空间使用 ==="
docker system df

echo ""
echo "=== LogService 数据大小（如果存在）==="
if [ -d "mo-data/logservice-data" ]; then
    du -sh mo-data/logservice-data/* 2>/dev/null | head -5
fi
```

## 推荐操作流程

### 立即解决（快速）

```bash
# 1. 清理 Docker 系统空间
docker system prune -a

# 2. 清理 Docker 日志
sudo find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 100M {} \;

# 3. 检查是否解决
df -h /var/lib/docker
```

### 长期解决（推荐）

```bash
# 1. 检查磁盘空间
df -h

# 2. 如果 /var/lib/docker 所在分区空间不足，迁移到更大的分区
# 参考方案 5 的完整迁移步骤

# 3. 配置 Docker 日志轮转
# 编辑 /etc/docker/daemon.json

# 4. 配置 LogService 减少数据生成
# 编辑 etc/launch-tae-compose/config/log.toml
```

## 注意事项

⚠️ **重要**:
- 迁移 Docker 数据前，**必须停止 Docker**
- 建议先备份数据
- 迁移后验证 Docker 是否正常工作
- 如果使用 systemd，确保 Docker 服务配置正确

## 验证

迁移或扩容后验证：

```bash
# 1. 检查 Docker 根目录
docker info | grep "Docker Root Dir"

# 2. 检查磁盘空间
df -h $(docker info | grep "Docker Root Dir" | awk '{print $4}')

# 3. 测试 Docker 功能
docker run hello-world

# 4. 检查 LogService 是否正常
docker compose --profile launch-multi-cn ps logservice
```











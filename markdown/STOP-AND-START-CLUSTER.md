# 停止和启动 MatrixOne 集群

## 停止集群（保留数据和配置）

### 方法1：停止所有服务（推荐）

```bash
cd etc/launch-tae-compose

# 停止所有服务（容器保留，数据卷保留）
docker compose --profile launch-multi-cn stop
```

**效果**：
- ✅ 停止所有容器（CN、TN、LogService、MinIO 等）
- ✅ 保留所有容器（不删除）
- ✅ 保留所有数据卷（数据不丢失）
- ✅ 保留所有网络配置
- ✅ 保留所有配置和日志

**下次启动**：
```bash
docker compose --profile launch-multi-cn start
# 或
docker compose --profile launch-multi-cn up -d
```

---

### 方法2：停止特定服务

如果只想停止部分服务：

```bash
# 只停止 CN
docker compose stop cn-0 cn-1

# 只停止 CN 和 TN
docker compose stop cn-0 cn-1 tn

# 停止所有服务
docker compose stop
```

---

## 启动集群

### 方法1：启动已停止的容器

如果之前使用 `docker compose stop` 停止的：

```bash
cd etc/launch-tae-compose

# 启动所有服务
docker compose --profile launch-multi-cn start

# 或使用 up（如果容器已存在，会启动它们）
docker compose --profile launch-multi-cn up -d
```

**效果**：
- ✅ 启动所有已存在的容器
- ✅ 使用之前的数据和配置
- ✅ 服务状态完全恢复

---

### 方法2：重新创建并启动

如果需要重新创建容器（但保留数据卷）：

```bash
cd etc/launch-tae-compose

# 停止并删除容器，但保留数据卷和网络
docker compose --profile launch-multi-cn down

# 重新启动（会创建新容器，但使用旧数据卷）
docker compose --profile launch-multi-cn up -d
```

**注意**：
- `docker compose down` 会删除容器，但**默认保留数据卷和网络**
- 数据不会丢失
- 容器会重新创建（使用最新镜像）

---

## 命令对比

| 命令 | 停止容器 | 删除容器 | 保留数据卷 | 保留网络 | 适用场景 |
|------|---------|---------|-----------|---------|---------|
| `docker compose stop` | ✅ | ❌ | ✅ | ✅ | **临时停止，下次继续使用** |
| `docker compose start` | - | - | ✅ | ✅ | 启动已停止的容器 |
| `docker compose down` | ✅ | ✅ | ✅ | ✅ | 停止并删除容器，但保留数据 |
| `docker compose down -v` | ✅ | ✅ | ❌ | ✅ | **完全清理，删除所有数据** |
| `docker compose up -d` | - | - | ✅ | ✅ | 启动/创建容器 |

---

## 完整工作流程

### 场景1：临时停止，稍后继续使用（推荐）

```bash
# 停止集群
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn stop

# 查看状态（容器还在，但已停止）
docker compose ps

# 稍后启动集群
docker compose --profile launch-multi-cn start

# 或
docker compose --profile launch-multi-cn up -d
```

### 场景2：停止并更新代码，然后重启

```bash
# 1. 停止集群
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn stop

# 2. 更新代码并构建新镜像
cd ../../
docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest \
  .

# 3. 重新启动（会使用新镜像）
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn up -d
```

### 场景3：完全清理并重新开始

```bash
# ⚠️ 警告：这会删除所有数据！
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn down -v

# 重新启动（创建全新的集群）
docker compose --profile launch-multi-cn up -d
```

---

## 验证集群状态

### 查看容器状态

```bash
# 查看所有容器状态
docker compose ps

# 查看特定服务状态
docker compose ps cn-0 cn-1

# 查看所有容器（包括已停止的）
docker compose ps -a
```

### 查看日志

```bash
# 查看所有服务日志
docker compose logs

# 查看特定服务日志
docker compose logs cn-0
docker compose logs -f cn-0  # 实时跟踪

# 查看最近100行日志
docker compose logs --tail=100 cn-0
```

### 检查服务健康状态

```bash
# 检查 CN0 是否可连接
mysql -h 127.0.0.1 -P 6001 -u root -p -e "SELECT 1;"

# 检查 HTTP 调试接口
curl http://localhost:12345/debug/pprof/

# 使用 mo_ctl 查看集群状态
docker exec -it cn-0 /mo-service mo_ctl cn list
```

---

## 常见问题

### Q1: `docker compose stop` 和 `docker compose down` 的区别？

**A**: 
- `stop`: 只停止容器，不删除。数据、网络、配置都保留。适合临时停止。
- `down`: 停止并删除容器，但默认保留数据卷和网络。适合需要重新创建容器的场景。

### Q2: 停止后数据会丢失吗？

**A**: 不会。使用 `docker compose stop` 或 `docker compose down`（不加 `-v`）都不会删除数据卷，数据完全保留。

### Q3: 停止后如何确认数据还在？

**A**: 
```bash
# 查看数据卷
docker volume ls

# 查看 MinIO 数据卷
docker volume inspect <minio_volume_name>

# 查看 LogService 数据卷
docker volume inspect logservice-data
```

### Q4: 停止后多久可以重新启动？

**A**: 随时可以。容器停止后可以立即启动，也可以几天、几周后启动，数据都会保留。

### Q5: 停止期间会占用资源吗？

**A**: 
- 容器停止后不占用 CPU 和内存
- 但数据卷仍占用磁盘空间
- 网络配置保留但不占用资源

### Q6: 如何只停止 CN，其他服务继续运行？

**A**: 
```bash
docker compose stop cn-0 cn-1
# TN、LogService、MinIO 继续运行
```

---

## 快速参考

### 停止集群
```bash
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn stop
```

### 启动集群
```bash
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn start
# 或
docker compose --profile launch-multi-cn up -d
```

### 查看状态
```bash
docker compose ps
```

### 查看日志
```bash
docker compose logs -f cn-0
```

---

## 总结

**推荐做法**（临时停止，下次继续使用）：

1. **停止**：`docker compose --profile launch-multi-cn stop`
2. **启动**：`docker compose --profile launch-multi-cn start` 或 `up -d`

**优点**：
- ✅ 数据完全保留
- ✅ 配置完全保留
- ✅ 启动速度快（不需要重新创建容器）
- ✅ 资源占用最小（停止后不占 CPU/内存）

**注意事项**：
- 停止期间数据卷仍占用磁盘空间
- 确保有足够的磁盘空间保存数据
- 定期备份重要数据（虽然数据不会丢失，但备份是好的实践）


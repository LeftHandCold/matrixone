# MatrixOne Docker 多CN集群部署指南

## 概述

本文档总结了使用 Docker 部署和管理 MatrixOne 多CN集群的完整流程，包括镜像构建、集群部署、数据管理、网络故障模拟等。

## 目录

1. [Docker镜像构建](#docker镜像构建)
2. [多CN集群部署](#多cn集群部署)
3. [容器管理](#容器管理)
4. [数据管理](#数据管理)
5. [网络故障模拟](#网络故障模拟)
6. [常见问题解决](#常见问题解决)

---

## Docker镜像构建

### 1. 构建镜像

使用 Dockerfile 构建 MatrixOne 镜像：

```bash
cd /path/to/matrixone
docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  --no-cache \
  -t matrixorigin/matrixone:latest .
```

### 2. GOPROXY 配置

**问题**：在中国大陆地区，直接访问 `proxy.golang.org` 可能超时。

**解决方案**：在 `optools/images/Dockerfile` 中配置多个 Go 代理镜像：

```dockerfile
# goproxy
ARG GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct"
RUN go env -w GOPROXY=${GOPROXY}
RUN go env -w GOSUMDB=sum.golang.google.cn
ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=sum.golang.google.cn
```

**关键点**：
- 使用多个国内镜像源，按优先级顺序尝试
- 设置 `GOSUMDB` 为国内镜像
- 使用 `ENV` 确保环境变量正确传递

### 3. Docker Registry 镜像配置

**问题**：拉取 Docker 镜像时超时（`context deadline exceeded`）。

**解决方案**：配置 Docker daemon 使用国内镜像源。

**方法1：修改 `/etc/docker/daemon.json`**（Linux）：

```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://mirror.aliyuncs.com",
    "https://dockerproxy.com",
    "https://mirror.baidubce.com"
  ],
  "insecure-registries": [
    "docker.mirrors.ustc.edu.cn",
    "registry.docker-cn.com",
    "127.0.0.0/8"
  ]
}
```

然后重启 Docker：
```bash
sudo systemctl restart docker
```

**方法2：Docker Desktop（macOS/Windows）**：
- 打开 Docker Desktop
- Settings → Docker Engine
- 添加上述配置到 JSON 中
- 点击 "Apply & Restart"

---

## 多CN集群部署

### 1. 使用 Docker Compose 部署

**位置**：`etc/launch-tae-compose/compose.yaml`

**启动命令**：
```bash
cd etc/launch-tae-compose
docker compose --profile launch-multi-cn up -d
```

**服务组件**：
- `logservice`: 日志服务
- `tn`: 事务节点
- `cn-0`: 计算节点0（协调节点）
- `cn-1`: 计算节点1
- `minio`: S3兼容存储
- `createbuckets`: 初始化存储桶

### 2. 查看集群状态

**查看容器状态**：
```bash
docker compose ps
```

**查看特定服务日志**：
```bash
docker compose logs -f cn-0
docker compose logs -f cn-1
docker compose logs -f logservice
```

**使用 mo_ctl 查看集群状态**：
```bash
docker exec -it cn-0 /mo-service mo_ctl cn list
```

**使用 HTTP 调试接口**：
```bash
# CN0: http://localhost:12345/debug/pprof/
# CN1: http://localhost:22345/debug/pprof/
# TN:  http://localhost:32345/debug/pprof/
# LogService: http://localhost:42345/debug/pprof/
```

### 3. 更新运行中的集群

**问题**：修改代码后，如何应用到运行中的集群？

**方法1：重建容器（推荐）**：
```bash
# 停止并删除容器（保留数据卷）
docker compose down

# 重新构建镜像（如果需要）
docker build -f optools/images/Dockerfile -t matrixorigin/matrixone:latest .

# 重新启动
docker compose --profile launch-multi-cn up -d
```

**方法2：仅重启容器（不更新镜像）**：
```bash
# 仅重启，不会使用新镜像
docker compose restart
```

**注意**：`docker compose restart` 不会重新拉取或构建镜像，只会重启现有容器。

---

## 容器管理

### 1. 进入容器

```bash
# 进入 CN0 容器
docker exec -it cn-0 /bin/bash

# 进入 CN1 容器
docker exec -it cn-1 /bin/bash

# 进入 LogService 容器
docker exec -it logservice /bin/bash
```

### 2. 查看日志位置

**容器内日志路径**：`/log/`

**宿主机映射路径**：`docker-compose-log/`（相对于 compose.yaml 所在目录）

**查看日志**：
```bash
# 在宿主机上
tail -f docker-compose-log/cn-0.log
tail -f docker-compose-log/cn-1.log
tail -f docker-compose-log/logservice.log

# 在容器内
tail -f /log/cn-0.log
```

### 3. 复制文件到/从容器

```bash
# 复制文件到容器
docker cp /path/to/local/file cn-0:/path/to/container/dest

# 从容器复制文件
docker cp cn-0:/path/to/container/file /path/to/local/dest
```

---

## 数据管理

### 1. MinIO 数据位置

**问题**：MinIO 数据存储在宿主机的哪里？

**答案**：MinIO 数据存储在 Docker 命名卷中。

**查找方法**：
```bash
# 查找 MinIO 数据卷
docker volume ls | grep minio

# 查看卷的详细信息
docker volume inspect <volume_name>

# 查看卷的实际存储位置（Linux）
docker volume inspect <volume_name> | grep Mountpoint
# 输出类似：/var/lib/docker/volumes/<volume_name>/_data
```

**备份 MinIO 数据**：
```bash
# 创建备份
docker run --rm -v <volume_name>:/data -v $(pwd):/backup \
  ubuntu:22.04 tar czf /backup/minio-backup.tar.gz /data

# 恢复备份
docker run --rm -v <volume_name>:/data -v $(pwd):/backup \
  ubuntu:22.04 tar xzf /backup/minio-backup.tar.gz -C /
```

### 2. LogService 数据位置

**问题**：LogService 数据存储在哪里？

**答案**：LogService 数据也存储在 Docker 命名卷中（`logservice-data`）。

**查看方法**：
```bash
docker volume inspect logservice-data
```

### 3. 使用本地数据文件

**场景**：需要在容器内使用宿主机上的数据文件（如 `LOAD DATA INFILE`）。

**解决方案1：挂载本地目录到容器**

在 `compose.yaml` 的 `x-mo-common` 中添加：
```yaml
x-mo-common: &mo-common
  volumes:
    - ../../etc/launch-tae-compose/config:/config
    - ../../test:/test
    - ../../docker-compose-log:/log
    - /home/mo/test/mo-tpcc/data:/data  # 添加本地数据目录
```

然后在容器内使用：
```sql
LOAD DATA INFILE '/data/order.csv' INTO TABLE bmsql_oorder
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';
```

**解决方案2：使用 `LOAD DATA LOCAL INFILE`**

如果文件在客户端机器上：
```bash
mysql -h 127.0.0.1 -P 6001 -u root -p --local-infile
```

```sql
LOAD DATA LOCAL INFILE '/path/to/local/file.csv' INTO TABLE t1
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
```

### 4. 清理所有数据并重建集群

**完全清理**：
```bash
# 停止并删除所有容器和网络
docker compose down

# 删除所有数据卷（⚠️ 警告：会删除所有数据）
docker compose down -v

# 或者只删除特定卷
docker volume rm <volume_name>

# 重新启动
docker compose --profile launch-multi-cn up -d
```

---

## 网络故障模拟

### 1. 断开 CN 网络连接

**场景**：模拟某个 CN 节点网络故障。

**方法1：使用 Docker 网络断开**：
```bash
# 查找容器所在的网络
docker inspect cn-1 | grep -A 10 Networks

# 断开网络（网络名通常是 launch-tae-compose_default 或类似）
docker network disconnect <network_name> cn-1

# 重新连接网络
docker network connect <network_name> cn-1
```

**方法2：使用脚本**（需要先找到正确的网络名）：
```bash
# 查找 CN1 所在的网络
NETWORK=$(docker inspect cn-1 --format='{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')

# 断开
docker network disconnect $NETWORK cn-1

# 重连
docker network connect $NETWORK cn-1
```

### 2. 模拟网络延迟

使用 `tc`（Traffic Control）在容器内模拟网络延迟：
```bash
# 进入容器
docker exec -it cn-1 /bin/bash

# 安装 tc（如果容器内没有）
apt-get update && apt-get install -y iproute2

# 添加延迟（100ms）
tc qdisc add dev eth0 root netem delay 100ms

# 移除延迟
tc qdisc del dev eth0 root
```

---

## 常见问题解决

### 1. LogService "no space left on device"

**问题**：LogService 容器报错 "no space left on device"。

**原因**：Docker 容器磁盘空间不足（通常是 Docker Desktop 的虚拟磁盘）。

**解决方案**：

**方法1：清理 Docker 空间**：
```bash
# 清理未使用的镜像、容器、网络
docker system prune -a

# 清理未使用的卷（⚠️ 注意：会删除未使用的数据卷）
docker volume prune
```

**方法2：扩展 Docker Desktop 磁盘大小**（macOS）：
- Docker Desktop → Settings → Resources → Advanced
- 增加 "Disk image size"（例如从 10GB 增加到 100GB）
- 点击 "Apply & Restart"

**方法3：将 LogService 数据挂载到宿主机**：
在 `compose.yaml` 的 `logservice` 服务中添加：
```yaml
logservice:
  volumes:
    - ../../etc/launch-tae-compose/config:/config
    - ../../test:/test
    - ../../docker-compose-log:/log
    - logservice-data:/var/lib/matrixone/data  # 命名卷
```

**注意**：如果直接添加 `volumes` 会覆盖继承的 volumes，需要显式列出所有需要的挂载。

### 2. 配置文件找不到

**问题**：`panic: failed to parse config from /config/log.toml: no such file or directory`

**原因**：在 `compose.yaml` 中直接添加 `volumes` 会覆盖从 `x-mo-common` 继承的 volumes。

**解决方案**：在服务中显式列出所有需要的 volumes：
```yaml
logservice:
  <<: *mo-common
  volumes:
    - ../../etc/launch-tae-compose/config:/config  # 必须显式列出
    - ../../test:/test
    - ../../docker-compose-log:/log
    - logservice-data:/var/lib/matrixone/data
```

### 3. 镜像构建超时

**问题**：`go mod download` 超时。

**解决方案**：
1. 使用多个国内 Go 代理镜像（见 [GOPROXY 配置](#2-goproxy-配置)）
2. 添加重试机制（在 Dockerfile 中）：
```dockerfile
RUN for i in 1 2 3 4 5; do \
      go mod download && break || sleep $i; \
    done
```

### 4. 容器无法访问宿主机文件

**问题**：容器内无法访问挂载的宿主机文件。

**检查清单**：
1. 确认 `compose.yaml` 中的 volumes 配置正确
2. 确认宿主机文件路径存在且可读
3. 检查文件权限（Linux 上可能需要 `chmod`）
4. 确认容器内路径正确

---

## 快速参考

### 常用命令

```bash
# 启动集群
docker compose --profile launch-multi-cn up -d

# 停止集群
docker compose down

# 查看日志
docker compose logs -f cn-0

# 进入容器
docker exec -it cn-0 /bin/bash

# 查看容器状态
docker compose ps

# 查看数据卷
docker volume ls

# 清理所有数据
docker compose down -v
```

### 端口映射

- **CN0**: 6001 (MySQL), 12345 (Debug HTTP)
- **CN1**: 7001 (MySQL), 22345 (Debug HTTP)
- **TN**: 32345 (Debug HTTP)
- **LogService**: 42345 (Debug HTTP)

### 数据卷

- **MinIO**: Docker 命名卷（自动创建）
- **LogService**: `logservice-data` 卷
- **日志**: `docker-compose-log/` 目录（宿主机）

---

## 总结

本文档涵盖了 MatrixOne Docker 多CN集群部署的完整流程，包括：

1. **镜像构建**：配置 Go 代理和 Docker 镜像源
2. **集群部署**：使用 Docker Compose 启动多CN集群
3. **容器管理**：进入容器、查看日志、复制文件
4. **数据管理**：MinIO/LogService 数据位置、本地文件使用、数据清理
5. **网络故障模拟**：断开网络、模拟延迟
6. **常见问题**：磁盘空间、配置文件、构建超时等

通过本文档，可以快速部署和管理 MatrixOne 多CN集群，并进行各种测试和故障模拟。


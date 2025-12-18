# 修复 Go 版本错误

## 问题描述

构建 Docker 镜像时遇到错误：
```
go: go.mod requires go >= 1.25.4 (running go 1.24.3; GOTOOLCHAIN=local)
The command '/bin/sh -c go mod download' returned a non-zero code: 1
```

**原因**：
- `go.mod` 要求 Go 版本 >= 1.25.4
- Docker 镜像中的 Go 版本是 1.24.3
- `GOTOOLCHAIN=local` 阻止了 Go 自动下载所需版本

---

## 解决方案

### 方法1：设置 GOTOOLCHAIN=auto（已修复）

在 Dockerfile 中添加 `GOTOOLCHAIN=auto`，让 Go 自动下载匹配的版本：

```dockerfile
FROM matrixorigin/golang:1.25-ubuntu22.04 AS builder

# goproxy
ARG GOPROXY="https://proxy.golang.org,direct"
RUN go env -w GOPROXY=${GOPROXY}
# Enable automatic toolchain download if local version doesn't match go.mod requirements
RUN go env -w GOTOOLCHAIN=auto
```

**优点**：
- ✅ 自动处理版本不匹配
- ✅ 无需手动更新基础镜像
- ✅ 适用于所有 Go 版本要求

**已修复的文件**：
- `optools/images/Dockerfile`
- `optools/bvt_ut/Dockerfile`

---

### 方法2：使用更具体的 Go 版本镜像

如果基础镜像标签不明确，可以使用更具体的版本：

```dockerfile
# 使用明确指定 Go 1.25.4 的镜像
FROM matrixorigin/golang:1.25.4-ubuntu22.04 AS builder
```

**注意**：需要确认该镜像标签是否存在。

---

### 方法3：在构建时设置环境变量

如果不想修改 Dockerfile，可以在构建时传递环境变量：

```bash
docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  --build-arg GOTOOLCHAIN=auto \
  -t matrixorigin/matrixone:latest .
```

然后在 Dockerfile 中使用：
```dockerfile
ARG GOTOOLCHAIN=auto
RUN go env -w GOTOOLCHAIN=${GOTOOLCHAIN}
```

---

## 验证修复

重新构建镜像：

```bash
cd /path/to/matrixone

docker build -f optools/images/Dockerfile \
  --build-arg GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct" \
  -t matrixorigin/matrixone:latest .
```

**预期结果**：
- ✅ `go mod download` 成功执行
- ✅ Go 自动下载 1.25.4 版本（如果镜像中版本不匹配）
- ✅ 构建过程正常进行

---

## GOTOOLCHAIN 说明

`GOTOOLCHAIN` 环境变量控制 Go 工具链的选择行为：

| 值 | 行为 |
|----|------|
| `local` | 只使用本地安装的 Go 版本，不自动下载 |
| `auto` | 如果本地版本不满足 `go.mod` 要求，自动下载匹配的版本 |
| `path` | 使用 `PATH` 中的 Go 工具链 |
| `go1.XX.X` | 使用指定的 Go 版本 |

**推荐**：在 Dockerfile 中使用 `GOTOOLCHAIN=auto`，这样可以：
- 自动处理版本不匹配
- 确保构建使用正确的 Go 版本
- 减少手动维护成本

---

## 常见问题

### Q1: 为什么镜像标签是 `1.25` 但实际版本是 `1.24.3`？

**A**: 镜像标签可能不精确，或者镜像没有及时更新。使用 `GOTOOLCHAIN=auto` 可以自动处理这种情况。

### Q2: 自动下载的 Go 版本会保存在哪里？

**A**: Go 会将工具链下载到 `$GOCACHE` 或 `$HOME/sdk` 目录。在 Docker 构建中，这些文件会在构建层中缓存。

### Q3: 会影响构建速度吗？

**A**: 
- 首次构建会下载 Go 工具链，可能稍慢
- 后续构建会使用缓存，速度不受影响
- 相比手动更新镜像，这种方式更灵活

### Q4: 本地开发环境也需要设置吗？

**A**: 
- 本地开发：如果本地 Go 版本满足要求，不需要设置
- 如果本地版本不满足，可以设置 `go env -w GOTOOLCHAIN=auto`
- Docker 构建：建议始终设置 `GOTOOLCHAIN=auto`

---

## 相关文件

- `optools/images/Dockerfile` - 主构建镜像
- `optools/bvt_ut/Dockerfile` - 测试镜像
- `go.mod` - Go 模块定义（要求 Go >= 1.25.4）

---

## 总结

**已修复**：在 Dockerfile 中添加了 `GOTOOLCHAIN=auto`，确保 Go 自动下载匹配的版本。

**下一步**：重新构建 Docker 镜像，应该可以正常通过 `go mod download` 步骤。


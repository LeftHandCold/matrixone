# OBS Simulator Proxy

模拟华为云 OBS 的两个 S3 兼容性问题，用于在本地 MinIO 环境验证 `parallel-mode` 和 `gc-delete-batch-size` 配置。

## 模拟的问题

1. **PutObject seekable**: 当请求使用 chunked transfer encoding（无 Content-Length）且数据 ≥ 64MB 时，返回 400 错误
2. **DeleteObjects MalformedXML**: 拦截批量删除请求（`?delete`），返回 400 MalformedXML

## 使用方法

```bash
# 1. 修改 MinIO 端口为 9100（proxy 占用 9000）
#    编辑 docker-compose.yml: ports "9100:9000"

# 2. 启动 MinIO
docker compose up -d

# 3. 启动 proxy（监听 9000，转发到 9100）
pip install aiohttp
python obs_simulator/obs_proxy.py

# 4. 正常启动 MO（MO 连 127.0.0.1:9000，实际经过 proxy）
make launch-minio

# 5. 验证：不加 parallel-mode 时 GC 写入应该报错
#    加上 parallel-mode = "1" 后应该正常
```

## 关闭模拟

直接 Ctrl+C 停止 proxy，把 MinIO 端口改回 9000 即可。

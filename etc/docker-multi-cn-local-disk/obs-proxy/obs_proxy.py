#!/usr/bin/env python3
"""
OBS Simulator Proxy (Docker version)

模拟华为云 OBS 的两个 S3 兼容性问题：
1. PutObject body >= 64MB 时，OBS 要求 body 可 seek（用于签名验证）。
2. DeleteObjects 批量删除的 XML 格式不兼容

Docker 环境中：
  - 监听 9000 端口（MO 容器连这个）
  - 转发到 minio:9000（MinIO 容器）

支持两种 S3 URL 风格：
  - Path-style:           http://obs-proxy:9000/mo-test/key
  - Virtual-hosted style: http://mo-test.obs-proxy:9000/key
    (AWS SDK v2 默认使用 virtual-hosted style)

关键设计：
  - PutObject ≥64MB 直接拦截返回 400，不转发
  - 所有需要转发的请求 body 都 < 64MB，用整体读取+一次性发送
  - 不使用流式转发，避免 asyncio 事件循环成为高并发瓶颈
  - DeleteObjects 需要读 body 检查 XML，但 body 很小
"""

import logging
import os

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("obs-proxy")

stats = {
    "total_requests": 0,
    "blocked_put_seekable": 0,
    "blocked_delete_multi": 0,
    "forwarded": 0,
    "virtual_hosted_rewrite": 0,
}


def make_s3_error_response(code: str, message: str, resource: str = "",
                           request_id: str = "OBS-SIM-001"):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
  <Resource>{resource}</Resource>
  <RequestId>{request_id}</RequestId>
</Error>"""
    return body


SMALL_OBJECT_THRESHOLD = 64 * 1024 * 1024  # 64MB


def extract_bucket_from_host(host: str, proxy_hostname: str) -> str | None:
    """Extract bucket name from virtual-hosted style Host header."""
    hostname = host.split(":")[0] if ":" in host else host
    suffix = f".{proxy_hostname}"
    if hostname.endswith(suffix):
        bucket = hostname[: -len(suffix)]
        if bucket:
            return bucket
    return None


def is_put_object(method: str, query: str, headers) -> bool:
    if method != "PUT":
        return False
    if "uploadid" in query.lower():
        return False
    if "x-amz-copy-source" in headers:
        return False
    return True


def is_delete_objects(method: str, query: str) -> bool:
    if method != "POST":
        return False
    return "delete" in query.lower()


def check_put_seekable_headers(method: str, query: str, headers,
                                path: str) -> web.Response | None:
    """Check PutObject headers only — no body read needed.

    Returns a 400 response if the request would fail on real OBS,
    or None if the request should be forwarded.
    """
    if not is_put_object(method, query, headers):
        return None

    cl_str = headers.get("Content-Length")
    te = headers.get("Transfer-Encoding", "").lower()

    # Case A: chunked or no Content-Length
    if "chunked" in te or cl_str is None:
        stats["blocked_put_seekable"] += 1
        log.warning(
            f"🚫 BLOCKED PutObject (chunked/no CL): {path} — OBS requires seekable body"
        )
        body = make_s3_error_response(
            code="InvalidRequest",
            message="failed to compute payload hash: failed to seek body to start, "
                    "request stream is not seekable",
            resource=path,
        )
        return web.Response(status=400, content_type="application/xml", text=body)

    # Case B: Content-Length >= 64MB
    try:
        content_length = int(cl_str)
    except (ValueError, TypeError):
        content_length = 0

    if content_length >= SMALL_OBJECT_THRESHOLD:
        stats["blocked_put_seekable"] += 1
        log.warning(
            f"🚫 BLOCKED PutObject (CL={content_length} >= 64MB): {path} "
            f"— MO sends raw io.Reader for large objects"
        )
        body = make_s3_error_response(
            code="InvalidRequest",
            message="failed to compute payload hash: failed to seek body to start, "
                    "request stream is not seekable",
            resource=path,
        )
        return web.Response(status=400, content_type="application/xml", text=body)

    return None


async def check_delete_objects_body(request: web.Request) -> web.Response | None:
    """Check DeleteObjects XML body — must read body (small)."""
    body_bytes = await request.read()
    body_str = body_bytes.decode("utf-8", errors="replace")
    obj_count = body_str.count("<Key>")
    if obj_count >= 1:
        stats["blocked_delete_multi"] += 1
        log.warning(
            f"🚫 BLOCKED DeleteObjects (batch={obj_count}): OBS MalformedXML"
        )
        body = make_s3_error_response(
            code="MalformedXML",
            message="The XML you provided was not well-formed or did not validate "
                    "against our published schema",
            resource=request.path,
            request_id="OBS-SIM-DELETE-001",
        )
        return web.Response(status=400, content_type="application/xml", text=body)
    return None


async def bulk_forward(request: web.Request, minio_url: str,
                       session: aiohttp.ClientSession) -> web.Response:
    """Read entire request body, forward to MinIO in one shot, return full response.

    Since PutObject >= 64MB is blocked (never forwarded), all forwarded requests
    have body < 64MB. Reading the full body into memory is safe and MUCH faster
    than streaming chunk-by-chunk through Python's asyncio event loop.

    This eliminates the per-chunk scheduling overhead that caused LOAD DATA
    operations to stall under high concurrency.
    """
    path_qs = request.path_qs

    # Forward ALL headers except hop-by-hop
    fwd_headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in ("transfer-encoding", "connection",
                                        "keep-alive", "te", "upgrade")}

    target_url = f"{minio_url}{path_qs}"

    # Read entire request body (safe: max 64MB, typically much smaller)
    req_body = await request.read()

    async with session.request(
        method=request.method,
        url=target_url,
        headers=fwd_headers,
        data=req_body if req_body else None,
        allow_redirects=False,
        timeout=aiohttp.ClientTimeout(total=600),
    ) as upstream_resp:
        # Read entire response body
        resp_body = await upstream_resp.read()

        # Build response with all upstream headers
        resp_headers = {k: v for k, v in upstream_resp.headers.items()
                        if k.lower() not in ("transfer-encoding", "connection",
                                             "keep-alive")}

        return web.Response(
            status=upstream_resp.status,
            headers=resp_headers,
            body=resp_body,
        )


async def handle_request(request: web.Request) -> web.Response:
    stats["total_requests"] += 1
    method = request.method
    path = request.path
    qs = request.query_string
    cl = request.headers.get("Content-Length", "?")
    te = request.headers.get("Transfer-Encoding", "")
    host = request.headers.get("Host", "")

    # Track virtual-hosted style
    proxy_hostname = request.app["proxy_hostname"]
    vhost_bucket = extract_bucket_from_host(host, proxy_hostname)
    if vhost_bucket:
        stats["virtual_hosted_rewrite"] += 1

    # Check 1: PutObject seekable — headers only, no body read
    blocked = check_put_seekable_headers(method, qs, request.headers, path)
    if blocked:
        return blocked

    # Check 2: DeleteObjects — needs body read (body is small XML)
    if is_delete_objects(method, qs):
        blocked = await check_delete_objects_body(request)
        if blocked:
            return blocked

    stats["forwarded"] += 1
    if method in ("PUT", "DELETE", "POST"):
        log.info(f"✅ FORWARD {method} Host={host} {path}{'?' + qs if qs else ''} CL={cl} TE={te}")

    # Bulk-forward to MinIO (read full body, send in one shot)
    session = request.app["client_session"]
    minio_url = request.app["minio_url"]
    return await bulk_forward(request, minio_url, session)


async def handle_stats(request: web.Request) -> web.Response:
    lines = [
        "=== OBS Simulator Stats ===",
        f"Total requests:          {stats['total_requests']}",
        f"Blocked (seekable):      {stats['blocked_put_seekable']}",
        f"Blocked (delete XML):    {stats['blocked_delete_multi']}",
        f"Virtual-hosted rewrites: {stats['virtual_hosted_rewrite']}",
        f"Forwarded:               {stats['forwarded']}",
    ]
    return web.Response(text="\n".join(lines))


async def on_startup(app: web.Application):
    """Create shared aiohttp ClientSession for connection pooling."""
    # Disable automatic Host header — we forward the original Host
    # so S3 Signature V4 stays valid and MinIO (MINIO_DOMAIN=obs-proxy)
    # can extract bucket from virtual-hosted style Host header.
    connector = aiohttp.TCPConnector(limit=0, force_close=False)
    app["client_session"] = aiohttp.ClientSession(
        connector=connector,
        auto_decompress=False,
        skip_auto_headers=["Host", "User-Agent", "Accept", "Accept-Encoding"],
    )


async def on_cleanup(app: web.Application):
    await app["client_session"].close()


def main():
    listen_port = int(os.environ.get("LISTEN_PORT", "9000"))
    minio_host = os.environ.get("MINIO_HOST", "minio")
    minio_port = int(os.environ.get("MINIO_PORT", "9000"))
    bucket_name = os.environ.get("BUCKET_NAME", "mo-test")
    proxy_hostname = os.environ.get("PROXY_HOSTNAME", "obs-proxy")

    minio_url = f"http://{minio_host}:{minio_port}"

    app = web.Application(client_max_size=0)
    app["minio_url"] = minio_url
    app["bucket_name"] = bucket_name
    app["proxy_hostname"] = proxy_hostname
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/__obs_stats", handle_stats)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    log.info(f"🚀 OBS Simulator Proxy starting")
    log.info(f"   Listen:  http://0.0.0.0:{listen_port}")
    log.info(f"   MinIO:   {minio_url}")
    log.info(f"   Bucket:  {bucket_name}")
    log.info(f"   转发方式: 整体读取+一次性发送（所有转发请求 body < 64MB）")
    log.info(f"   URL 风格: path-style + virtual-hosted ({bucket_name}.{proxy_hostname})")
    log.info(f"   模拟规则:")
    log.info(f"     1. PutObject CL >= 64MB → 400 seekable error")
    log.info(f"     2. PutObject chunked/无 CL → 400 seekable error")
    log.info(f"     3. DeleteObjects XML     → 400 MalformedXML")
    web.run_app(app, host="0.0.0.0", port=listen_port, print=None)


if __name__ == "__main__":
    main()

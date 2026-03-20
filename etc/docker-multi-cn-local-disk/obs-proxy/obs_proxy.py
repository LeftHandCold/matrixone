#!/usr/bin/env python3
"""
OBS Simulator Proxy (Docker version)

模拟华为云 OBS 的两个 S3 兼容性问题：
1. PutObject body >= 64MB 时，OBS 要求 body 可 seek（用于签名验证）。
   MO 的 aws_sdk_v2.Write 在 sizeHint >= 64MB 时直接传 raw io.Reader（不可 seek），
   即使设置了 Content-Length，OBS 也会拒绝。
   而 sizeHint < 64MB 时 MO 会先 ReadAll 到 bytes.Reader（可 seek），不会触发此问题。
2. DeleteObjects 批量删除的 XML 格式不兼容

Docker 环境中：
  - 监听 9000 端口（MO 容器连这个）
  - 转发到 minio:9000（MinIO 容器）

支持两种 S3 URL 风格：
  - Path-style:           http://obs-proxy:9000/mo-test/key
  - Virtual-hosted style: http://mo-test.obs-proxy:9000/key
    (AWS SDK v2 默认使用 virtual-hosted style)

关键设计：使用 http.client 做 raw 转发，完全透传所有 headers（包括 Host、
Content-Length、Authorization 等），不让任何 HTTP 框架自动修改 headers，
确保 S3 Signature V4 签名不被破坏。
"""

import asyncio
import http.client
import logging
import os

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


# MO's aws_sdk_v2.Write uses this threshold to decide the write path:
#   sizeHint == nil  → serial multipart upload (each part 64MB, bytes.Reader, seekable) → OK
#   sizeHint < 64MB  → ReadAll to bytes.Reader + PutObject → seekable → OK
#   sizeHint >= 64MB → PutObject with raw io.Reader → NOT seekable → OBS fails
# So we block PutObject when Content-Length >= this threshold.
SMALL_OBJECT_THRESHOLD = 64 * 1024 * 1024  # 64MB, matches fileservice.smallObjectThreshold


def extract_bucket_from_host(host: str, proxy_hostname: str) -> str | None:
    """Extract bucket name from virtual-hosted style Host header.

    AWS SDK v2 virtual-hosted style:
      Host: <bucket>.<endpoint-hostname>:<port>
      e.g.: mo-test.obs-proxy:9000

    Returns bucket name if virtual-hosted style detected, None if path-style.
    """
    # Strip port
    hostname = host.split(":")[0] if ":" in host else host

    # If hostname ends with .<proxy_hostname>, it's virtual-hosted style
    # e.g. "mo-test.obs-proxy" → bucket = "mo-test"
    suffix = f".{proxy_hostname}"
    if hostname.endswith(suffix):
        bucket = hostname[: -len(suffix)]
        if bucket:
            return bucket

    return None


def is_put_object(request: web.Request) -> bool:
    if request.method != "PUT":
        return False
    query = request.query_string.lower()
    if "uploadid" in query:
        return False
    if "x-amz-copy-source" in request.headers:
        return False
    return True


def is_delete_objects(request: web.Request) -> bool:
    if request.method != "POST":
        return False
    return "delete" in request.query_string.lower()


async def check_put_seekable(request: web.Request) -> web.Response | None:
    """Block PutObject requests that would fail on real OBS.

    Real OBS requires the request body to be seekable for signature verification.
    MO's Write function has three code paths:
      1. sizeHint == nil  → multipart upload (each part uses bytes.Reader = seekable) → OK
      2. sizeHint < 64MB  → ReadAll into bytes.Reader + PutObject → seekable → OK
      3. sizeHint >= 64MB → PutObject with raw io.Reader → NOT seekable → OBS FAILS

    We simulate case 3 by blocking PutObject when Content-Length >= 64MB.
    We also block chunked/no-Content-Length as a safety net (though MO doesn't
    normally produce this pattern).
    """
    if not is_put_object(request):
        return None

    cl_str = request.headers.get("Content-Length")
    te = request.headers.get("Transfer-Encoding", "").lower()

    # Case A: chunked or no Content-Length — body is definitely not seekable
    if "chunked" in te or cl_str is None:
        path = request.path
        stats["blocked_put_seekable"] += 1
        log.warning(
            f"🚫 BLOCKED PutObject (chunked/no Content-Length): {path} "
            f"— OBS requires seekable body"
        )
        body = make_s3_error_response(
            code="InvalidRequest",
            message="failed to compute payload hash: failed to seek body to start, "
                    "request stream is not seekable",
            resource=path,
        )
        return web.Response(status=400, content_type="application/xml", text=body)

    # Case B: Content-Length >= 64MB — MO passes raw io.Reader (not seekable)
    # This is the actual OBS failure path in production
    try:
        content_length = int(cl_str)
    except (ValueError, TypeError):
        content_length = 0

    if content_length >= SMALL_OBJECT_THRESHOLD:
        path = request.path
        stats["blocked_put_seekable"] += 1
        log.warning(
            f"🚫 BLOCKED PutObject (Content-Length={content_length} >= 64MB): {path} "
            f"— OBS requires seekable body, but MO sends raw io.Reader for large objects"
        )
        body = make_s3_error_response(
            code="InvalidRequest",
            message="failed to compute payload hash: failed to seek body to start, "
                    "request stream is not seekable",
            resource=path,
        )
        return web.Response(status=400, content_type="application/xml", text=body)

    return None


async def check_delete_objects(request: web.Request) -> web.Response | None:
    if not is_delete_objects(request):
        return None
    body_bytes = await request.read()
    body_str = body_bytes.decode("utf-8", errors="replace")
    obj_count = body_str.count("<Key>")
    if obj_count >= 1:
        stats["blocked_delete_multi"] += 1
        log.warning(
            f"🚫 BLOCKED DeleteObjects (batch={obj_count}): "
            f"OBS does not support DeleteObjects XML"
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


def raw_forward(method: str, path_qs: str, headers: list[tuple[str, str]],
                body: bytes | None, minio_host: str, minio_port: int
                ) -> tuple[int, list[tuple[str, str]], bytes]:
    """Forward request to MinIO using http.client — zero header mutation.

    We use http.client directly (not aiohttp, not requests, not urllib3) because
    it gives us full control over the wire format. We pass ALL original headers
    exactly as received, including Host and Authorization, so S3 Signature V4
    verification on MinIO side sees the exact same headers the client signed.
    """
    conn = http.client.HTTPConnection(minio_host, minio_port, timeout=300)
    try:
        conn.putrequest(method, path_qs, skip_host=True, skip_accept_encoding=True)
        for name, value in headers:
            conn.putheader(name, value)
        conn.endheaders(body or b"")
        resp = conn.getresponse()
        resp_body = resp.read()
        resp_headers = [(k, v) for k, v in resp.getheaders()
                        if k.lower() not in ("transfer-encoding", "connection")]
        return resp.status, resp_headers, resp_body
    finally:
        conn.close()


async def forward_request(request: web.Request, minio_host: str,
                          minio_port: int) -> web.Response:
    body = await request.read()

    path_qs = request.path_qs

    # Collect ALL headers from original request — no filtering, no mutation.
    # Host, Authorization, Content-Length, x-amz-* — everything goes through.
    #
    # For virtual-hosted style (Host=mo-test.obs-proxy:9000, path=/key):
    #   - We keep Host as-is so S3 Signature V4 stays valid
    #   - We keep path as-is (/key, not /mo-test/key)
    #   - MinIO has MINIO_DOMAIN=obs-proxy, so it extracts bucket="mo-test"
    #     from Host header and resolves the object correctly
    #   - If we rewrote path to /mo-test/key, the signature would break
    #     because S3 SigV4 signs the canonical URI (original path)
    #
    # TCP connection goes to minio:9000 (via raw_forward), but HTTP Host
    # header stays as the client sent it.
    headers = [(k, v) for k, v in request.headers.items()
               if k.lower() not in ("transfer-encoding", "connection")]

    # Log virtual-hosted style detection for debugging
    proxy_hostname = request.app["proxy_hostname"]
    vhost_bucket = extract_bucket_from_host(
        request.headers.get("Host", ""), proxy_hostname
    )
    if vhost_bucket:
        stats["virtual_hosted_rewrite"] += 1
        log.debug(
            f"🔄 Virtual-hosted style detected: Host={request.headers.get('Host')} "
            f"bucket={vhost_bucket} path={path_qs}"
        )

    # Run synchronous http.client call in thread pool to avoid blocking event loop
    loop = asyncio.get_event_loop()
    status, resp_headers, resp_body = await loop.run_in_executor(
        None, raw_forward, request.method, path_qs, headers, body,
        minio_host, minio_port
    )

    resp = web.Response(status=status, body=resp_body)
    for name, value in resp_headers:
        resp.headers[name] = value
    return resp


async def handle_request(request: web.Request) -> web.Response:
    stats["total_requests"] += 1
    minio_host = request.app["minio_host"]
    minio_port = request.app["minio_port"]
    method = request.method
    path = request.path
    qs = request.query_string
    cl = request.headers.get("Content-Length", "?")
    te = request.headers.get("Transfer-Encoding", "")
    host = request.headers.get("Host", "")

    blocked = await check_put_seekable(request)
    if blocked:
        return blocked
    blocked = await check_delete_objects(request)
    if blocked:
        return blocked

    stats["forwarded"] += 1
    if method in ("PUT", "DELETE", "POST"):
        log.info(f"✅ FORWARD {method} Host={host} {path}{'?' + qs if qs else ''} CL={cl} TE={te}")
    return await forward_request(request, minio_host, minio_port)


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


def main():
    listen_port = int(os.environ.get("LISTEN_PORT", "9000"))
    minio_host = os.environ.get("MINIO_HOST", "minio")
    minio_port = int(os.environ.get("MINIO_PORT", "9000"))
    bucket_name = os.environ.get("BUCKET_NAME", "mo-test")
    # The hostname that MO uses in endpoint config (e.g. "obs-proxy")
    # Virtual-hosted style will be: <bucket>.<proxy_hostname>:<port>
    proxy_hostname = os.environ.get("PROXY_HOSTNAME", "obs-proxy")

    app = web.Application(client_max_size=0)
    app["minio_host"] = minio_host
    app["minio_port"] = minio_port
    app["bucket_name"] = bucket_name
    app["proxy_hostname"] = proxy_hostname
    app.router.add_get("/__obs_stats", handle_stats)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    log.info(f"🚀 OBS Simulator Proxy starting")
    log.info(f"   Listen:  http://0.0.0.0:{listen_port}")
    log.info(f"   MinIO:   http://{minio_host}:{minio_port}")
    log.info(f"   Bucket:  {bucket_name}")
    log.info(f"   Proxy hostname: {proxy_hostname}")
    log.info(f"   转发方式: http.client raw (保留所有原始 headers，不破坏 S3 签名)")
    log.info(f"   URL 风格支持:")
    log.info(f"     - Path-style:    http://{proxy_hostname}:{listen_port}/{bucket_name}/key")
    log.info(f"     - Virtual-hosted: http://{bucket_name}.{proxy_hostname}:{listen_port}/key")
    log.info(f"   模拟规则:")
    log.info(f"     1. PutObject Content-Length >= 64MB → 400 seekable error")
    log.info(f"        (MO 的 sizeHint >= 64MB 走 raw io.Reader，不可 seek)")
    log.info(f"     2. PutObject chunked/无 Content-Length → 400 seekable error")
    log.info(f"     3. DeleteObjects XML                   → 400 MalformedXML")
    web.run_app(app, host="0.0.0.0", port=listen_port, print=None)


if __name__ == "__main__":
    main()

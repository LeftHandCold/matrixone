#!/usr/bin/env python3
"""
OBS Simulator Proxy (Docker version)

模拟华为云 OBS 的两个 S3 兼容性问题：
1. PutObject 要求 Body 可 seek（有 Content-Length），chunked 传输会被拒绝
2. DeleteObjects 批量删除的 XML 格式不兼容

Docker 环境中：
  - 监听 9000 端口（MO 容器连这个）
  - 转发到 minio:9000（MinIO 容器）

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


def is_chunked_transfer(request: web.Request) -> bool:
    te = request.headers.get("Transfer-Encoding", "").lower()
    has_content_length = "Content-Length" in request.headers
    return "chunked" in te or not has_content_length


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
    if not is_put_object(request):
        return None
    if is_chunked_transfer(request):
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

    # Collect ALL headers from original request — no filtering at all.
    # Host, Authorization, Content-Length, x-amz-* — everything goes through.
    headers = [(k, v) for k, v in request.headers.items()
               if k.lower() not in ("transfer-encoding", "connection")]

    path_qs = request.path_qs

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

    blocked = await check_put_seekable(request)
    if blocked:
        return blocked
    blocked = await check_delete_objects(request)
    if blocked:
        return blocked

    stats["forwarded"] += 1
    if method in ("PUT", "DELETE", "POST"):
        log.info(f"✅ FORWARD {method} {path}{'?' + qs if qs else ''} CL={cl} TE={te}")
    return await forward_request(request, minio_host, minio_port)


async def handle_stats(request: web.Request) -> web.Response:
    lines = [
        "=== OBS Simulator Stats ===",
        f"Total requests:        {stats['total_requests']}",
        f"Blocked (seekable):    {stats['blocked_put_seekable']}",
        f"Blocked (delete XML):  {stats['blocked_delete_multi']}",
        f"Forwarded:             {stats['forwarded']}",
    ]
    return web.Response(text="\n".join(lines))


def main():
    listen_port = int(os.environ.get("LISTEN_PORT", "9000"))
    minio_host = os.environ.get("MINIO_HOST", "minio")
    minio_port = int(os.environ.get("MINIO_PORT", "9000"))

    app = web.Application(client_max_size=0)
    app["minio_host"] = minio_host
    app["minio_port"] = minio_port
    app.router.add_get("/__obs_stats", handle_stats)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    log.info(f"🚀 OBS Simulator Proxy starting")
    log.info(f"   Listen:  http://0.0.0.0:{listen_port}")
    log.info(f"   MinIO:   http://{minio_host}:{minio_port}")
    log.info(f"   转发方式: http.client raw (保留所有原始 headers，不破坏 S3 签名)")
    log.info(f"   模拟规则:")
    log.info(f"     1. PutObject 无 Content-Length → 400 seekable error")
    log.info(f"     2. DeleteObjects XML           → 400 MalformedXML")
    web.run_app(app, host="0.0.0.0", port=listen_port, print=None)


if __name__ == "__main__":
    main()

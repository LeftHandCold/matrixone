#!/usr/bin/env python3
"""
OBS Simulator Proxy (Docker version)

模拟华为云 OBS 的两个 S3 兼容性问题：
1. PutObject 要求 Body 可 seek（有 Content-Length），chunked 传输会被拒绝
2. DeleteObjects 批量删除的 XML 格式不兼容

Docker 环境中：
  - 监听 9000 端口（MO 容器连这个）
  - 转发到 minio:9000（MinIO 容器）
"""

import argparse
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
}

SEEKABLE_THRESHOLD = 64 * 1024 * 1024  # 64MB


def make_s3_error_response(code: str, message: str, resource: str = "", request_id: str = "OBS-SIM-001"):
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
            message="failed to compute payload hash: failed to seek body to start, request stream is not seekable",
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
            message="The XML you provided was not well-formed or did not validate against our published schema",
            resource=request.path,
            request_id="OBS-SIM-DELETE-001",
        )
        return web.Response(status=400, content_type="application/xml", text=body)
    return None


async def forward_request(request: web.Request, minio_base: str) -> web.Response:
    target_url = f"{minio_base}{request.path_qs}"
    body = await request.read()
    skip_headers = {"host", "transfer-encoding", "connection"}
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in skip_headers
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=body if body else None,
            allow_redirects=False,
        ) as resp:
            resp_body = await resp.read()
            resp_headers = {}
            skip_resp = {"transfer-encoding", "connection", "content-encoding"}
            for k, v in resp.headers.items():
                if k.lower() not in skip_resp:
                    resp_headers[k] = v
            return web.Response(status=resp.status, headers=resp_headers, body=resp_body)


async def handle_request(request: web.Request) -> web.Response:
    stats["total_requests"] += 1
    minio_base = request.app["minio_base"]
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
    return await forward_request(request, minio_base)


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
    minio_base = f"http://{minio_host}:{minio_port}"

    app = web.Application(client_max_size=0)
    app["minio_base"] = minio_base
    app.router.add_get("/__obs_stats", handle_stats)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    log.info(f"🚀 OBS Simulator Proxy starting")
    log.info(f"   Listen:  http://0.0.0.0:{listen_port}")
    log.info(f"   MinIO:   {minio_base}")
    log.info(f"   模拟规则:")
    log.info(f"     1. PutObject 无 Content-Length → 400 seekable error")
    log.info(f"     2. DeleteObjects XML           → 400 MalformedXML")
    web.run_app(app, host="0.0.0.0", port=listen_port, print=None)


if __name__ == "__main__":
    main()

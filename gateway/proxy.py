"""
IIISConnect HTTP/HTTPS Forward Proxy — port 3128

Replaces the slow 192.168.3.226:7890 proxy entirely.
ALL traffic routes through the IIISConnect agent on the iiis cluster!

Routing:
  - Accelerated domains -> Gateway /fetch (Pipeline mode, caching)
  - Other domains -> TCP tunnel through Agent (via WebSocket)
"""

import asyncio
import logging
import os
from urllib.parse import urlparse

import httpx
import websockets

log = logging.getLogger("proxy")

PROXY_HOST = os.getenv("IIISCONNECT_PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("IIISCONNECT_PROXY_PORT", "3128"))
GATEWAY_API = os.getenv("IIISCONNECT_GATEWAY_API", "http://127.0.0.1:8000")
GATEWAY_WS = os.getenv("IIISCONNECT_GATEWAY_WS", "ws://127.0.0.1:8000")

ACCELERATED_DOMAINS = {
    "huggingface.co", "hf-mirror.com", "cdn-lfs.huggingface.co",
    "cdn-lfs-us-1.huggingface.co", "cdn-lfs-eu-1.huggingface.co",
    "github.com", "objects.githubusercontent.com", "raw.githubusercontent.com",
    "files.pythonhosted.org", "pypi.org", "registry.npmjs.org",
}

def is_accelerated(host: str) -> bool:
    host = host.split(":")[0].lower()
    return any(host == d or host.endswith("." + d) for d in ACCELERATED_DOMAINS)

async def open_agent_tunnel(host: str, port: int):
    ws_url = f"{GATEWAY_WS}/ws/tunnel_req/{host}/{port}"
    return await websockets.connect(
        ws_url,
        max_size=20*1024*1024,
        ping_interval=None,
        ping_timeout=None
    )

async def handle_connect(host: str, port: int, reader, writer):
    log.info(f"[CONNECT] {host}:{port} via Agent Tunnel")
    try:
        ws = await asyncio.wait_for(open_agent_tunnel(host, port), timeout=15)
    except Exception as e:
        log.error(f"[CONNECT] tunnel to {host}:{port} failed: {e}")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        return

    writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
    await writer.drain()

    async def r2w():
        try:
            while True:
                data = await reader.read(65536)
                if not data: break
                await ws.send(data)
        except: pass

    async def w2r():
        try:
            async for msg in ws:
                writer.write(msg)
                await writer.drain()
        except: pass

    await asyncio.gather(r2w(), w2r())
    try: await ws.close()
    except: pass

async def handle_http_request(method, url, version, headers, body, reader, writer):
    parsed = urlparse(url)
    host = parsed.hostname or ""

    if method.upper() == "GET" and is_accelerated(host):
        log.info(f"[HTTP-ACC] {url} via Pipeline")
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(1800, connect=30), follow_redirects=True) as client:
                async with client.stream("GET", f"{GATEWAY_API}/fetch", params={"url": url}) as gw_resp:
                    if gw_resp.status_code >= 400:
                        log.warning(f"[HTTP-ACC] Gateway returned {gw_resp.status_code} for {url}")
                        # Fall through to tunnel
                        raise httpx.HTTPStatusError(
                            f"Gateway error {gw_resp.status_code}",
                            request=gw_resp.request,
                            response=gw_resp,
                        )
                    status_line = f"HTTP/1.1 {gw_resp.status_code} OK\r\n"
                    resp_headers = "Content-Type: application/octet-stream\r\n"
                    if "content-length" in gw_resp.headers:
                        resp_headers += f"Content-Length: {gw_resp.headers['content-length']}\r\n"
                    resp_headers += "Connection: close\r\n\r\n"
                    writer.write((status_line + resp_headers).encode())
                    await writer.drain()
                    total_sent = 0
                    async for chunk in gw_resp.aiter_bytes(65536):
                        writer.write(chunk)
                        await writer.drain()
                        total_sent += len(chunk)
                    log.info(f"[HTTP-ACC] Delivered {total_sent / 1e6:.1f} MB for {url}")
            return
        except Exception as e:
            log.warning(f"[HTTP-ACC] Failed for {url}: {e}, falling back to tunnel")

    # Tunnel plain HTTP via agent
    port = parsed.port or 80
    log.info(f"[HTTP-TUNNEL] {host}:{port} via Agent Tunnel")
    try:
        ws = await asyncio.wait_for(open_agent_tunnel(host, port), timeout=15)
    except Exception as e:
        log.error(f"[HTTP-TUNNEL] failed: {e}")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        return

    path = parsed.path or "/"
    if parsed.query: path += "?" + parsed.query
    req = f"{method} {path} {version}\r\n"
    for k, v in headers:
        if k.lower() not in ('proxy-connection',):
            req += f"{k}: {v}\r\n"
    req += "Connection: close\r\n\r\n"

    await ws.send(req.encode() + body)

    async def r2w():
        try:
            while True:
                data = await reader.read(65536)
                if not data: break
                await ws.send(data)
        except: pass

    async def w2r():
        try:
            async for msg in ws:
                writer.write(msg)
                await writer.drain()
        except: pass

    await asyncio.gather(r2w(), w2r())
    try: await ws.close()
    except: pass

async def handle_proxy_client(reader, writer):
    try:
        header_data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)
        text = header_data.decode(errors="replace")
        lines = text.strip().split("\r\n")
        if not lines: return

        req_line = lines[0].split()
        if len(req_line) < 3: return
        method, target, version = req_line[0], req_line[1], req_line[2]

        headers = []
        content_length = 0
        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                headers.append((k.strip(), v.strip()))
                if k.strip().lower() == "content-length":
                    content_length = int(v.strip())

        body = await reader.read(content_length) if content_length > 0 else b""

        if method.upper() == "CONNECT":
            h, p = target.rsplit(":", 1) if ":" in target else (target, 443)
            await handle_connect(h, int(p), reader, writer)
        else:
            await handle_http_request(method, target, version, headers, body, reader, writer)

    except Exception:
        pass
    finally:
        try: writer.close()
        except: pass

async def run_proxy():
    server = await asyncio.start_server(handle_proxy_client, PROXY_HOST, PROXY_PORT)
    log.info(f"HTTP/HTTPS proxy listening on {PROXY_HOST}:{PROXY_PORT}")
    log.info("Routing ALL traffic through IIISConnect Agent tunnel (NO UPSTREAM PROXY)")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_proxy())

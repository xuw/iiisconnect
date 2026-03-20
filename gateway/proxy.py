"""
IIISConnect HTTP/HTTPS Proxy — runs alongside gateway on port 3128.

Usage (in xshixun pods):
    export http_proxy=http://iiisconnect-gateway:3128
    export https_proxy=http://iiisconnect-gateway:3128
    export no_proxy=localhost,127.0.0.1,.svc,.cluster

Then pip, curl, wget, git clone, huggingface_hub, etc. all route through here.

Routing logic:
  - Requests for URLs with a known IIISConnect mirror (HuggingFace, GitHub releases,
    PyPI, etc.) are served via IIISConnect pipeline (fast, cached, accelerated).
  - Everything else is forwarded as a standard transparent proxy (using the
    cluster's outbound via 192.168.3.226:7890 if needed).

For HTTPS (CONNECT tunnel):
  - Check if host is a known accelerated domain — if yes, wait for the file
    to arrive via IIISConnect and serve it (only works for direct file GETs).
  - Otherwise: transparent TCP tunnel (no MITM, no cert needed).

Port: 3128
"""

import asyncio
import hashlib
import logging
import os
import time
import uuid
from urllib.parse import urlparse, urlencode

import httpx

log = logging.getLogger("proxy")

PROXY_HOST = os.getenv("IIISCONNECT_PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("IIISCONNECT_PROXY_PORT", "3128"))

# Gateway REST API (loopback — proxy and gateway share the same pod)
GATEWAY_API = os.getenv("IIISCONNECT_GATEWAY_API", "http://127.0.0.1:8000")

# Upstream proxy for domains we don't accelerate (xshixun outbound)
UPSTREAM_PROXY = os.getenv("UPSTREAM_PROXY", "http://192.168.3.226:7890")

# Domains we can accelerate via IIISConnect agent
ACCELERATED_DOMAINS = {
    "huggingface.co",
    "hf-mirror.com",
    "cdn-lfs.huggingface.co",
    "cdn-lfs-us-1.huggingface.co",
    "github.com",
    "objects.githubusercontent.com",
    "files.pythonhosted.org",
    "pypi.org",
    "registry.npmjs.org",
}

# How long to poll gateway for task completion (seconds)
POLL_TIMEOUT = 1800
POLL_INTERVAL = 1.0


def is_accelerated(host: str) -> bool:
    host = host.split(":")[0].lower()
    for d in ACCELERATED_DOMAINS:
        if host == d or host.endswith("." + d):
            return True
    return False


async def gateway_download(url: str) -> tuple[str, asyncio.StreamReader]:
    """
    Submit URL to IIISConnect gateway and stream back the file content.
    Returns (filename, async_generator_of_bytes).
    Raises on failure.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{GATEWAY_API}/download", json={"url": url})
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") == "completed":
        # Cache hit — stream directly via /fetch
        task_id = data["task_id"]
        filename = data.get("path", url).split("/")[-1]
        return filename, task_id, True  # (filename, task_id, cache_hit)

    task_id = data["task_id"]
    filename = url.split("/")[-1] or "file"
    return filename, task_id, False


async def poll_until_done(task_id: str) -> dict:
    """Poll gateway status until task completes or fails."""
    deadline = time.time() + POLL_TIMEOUT
    async with httpx.AsyncClient(timeout=5) as client:
        while time.time() < deadline:
            try:
                resp = await client.get(f"{GATEWAY_API}/status/{task_id}")
                if resp.status_code == 200:
                    t = resp.json()
                    if t["status"] == "completed":
                        return t
                    if t["status"] == "failed":
                        raise RuntimeError(f"IIISConnect task failed: {t.get('error', '?')}")
            except (httpx.RequestError, RuntimeError):
                raise
            except Exception:
                pass
            await asyncio.sleep(POLL_INTERVAL)
    raise TimeoutError(f"IIISConnect task {task_id} timed out after {POLL_TIMEOUT}s")


async def stream_from_gateway(url: str, writer: asyncio.StreamWriter):
    """
    Submit download to gateway, wait for completion, stream file back to client.
    """
    async with httpx.AsyncClient(timeout=POLL_TIMEOUT + 60, follow_redirects=True) as client:
        async with client.stream("GET", f"{GATEWAY_API}/fetch",
                                 params={"url": url}) as resp:
            if resp.status_code >= 400:
                raise RuntimeError(f"Gateway /fetch returned {resp.status_code}")
            async for chunk in resp.aiter_bytes(65536):
                writer.write(chunk)
                await writer.drain()


# ---------------------------------------------------------------------------
# HTTP proxy (plain HTTP CONNECT not needed — client sends full URL)
# ---------------------------------------------------------------------------

async def handle_http_request(method: str, url: str, http_version: str,
                               headers: list[tuple[str, str]],
                               body: bytes,
                               reader: asyncio.StreamReader,
                               writer: asyncio.StreamWriter):
    """Handle a plain HTTP proxy request (non-CONNECT)."""
    parsed = urlparse(url)
    host = parsed.hostname or ""

    if method.upper() == "GET" and is_accelerated(host):
        log.info(f"[HTTP-ACC] {url}")
        try:
            # Use /fetch which blocks until file is ready, then streams it
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(POLL_TIMEOUT + 60, connect=10),
                follow_redirects=True,
            ) as client:
                async with client.stream(
                    "GET", f"{GATEWAY_API}/fetch", params={"url": url}
                ) as gw_resp:
                    # Send response header
                    status_line = f"HTTP/1.1 {gw_resp.status_code} OK\r\n"
                    resp_headers = "Content-Type: application/octet-stream\r\n"
                    cl = gw_resp.headers.get("content-length", "")
                    if cl:
                        resp_headers += f"Content-Length: {cl}\r\n"
                    cd = gw_resp.headers.get("content-disposition", "")
                    if cd:
                        resp_headers += f"Content-Disposition: {cd}\r\n"
                    resp_headers += "X-IIISConnect: accelerated\r\n"
                    resp_headers += "Connection: close\r\n\r\n"
                    writer.write((status_line + resp_headers).encode())
                    await writer.drain()

                    async for chunk in gw_resp.aiter_bytes(65536):
                        writer.write(chunk)
                        await writer.drain()
            return
        except Exception as e:
            log.warning(f"[HTTP-ACC] Failed for {url}: {e}, falling through to upstream")

    # Fallback: forward to upstream proxy
    await forward_http_upstream(method, url, http_version, headers, body, writer)


async def forward_http_upstream(method: str, url: str, http_version: str,
                                 headers: list[tuple[str, str]],
                                 body: bytes,
                                 writer: asyncio.StreamWriter):
    """Forward HTTP request to upstream proxy."""
    parsed = urlparse(UPSTREAM_PROXY)
    up_host = parsed.hostname
    up_port = parsed.port or 8080
    try:
        up_reader, up_writer = await asyncio.open_connection(up_host, up_port)
        # Reconstruct request
        req_line = f"{method} {url} HTTP/1.1\r\n"
        hdr_str = "".join(f"{k}: {v}\r\n" for k, v in headers if k.lower() != "proxy-connection")
        hdr_str += "Proxy-Connection: keep-alive\r\n\r\n"
        up_writer.write((req_line + hdr_str).encode())
        if body:
            up_writer.write(body)
        await up_writer.drain()

        # Pipe response back
        async def pipe():
            try:
                while True:
                    data = await up_reader.read(65536)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            except Exception:
                pass

        await pipe()
        up_writer.close()
    except Exception as e:
        log.error(f"[HTTP-FWD] upstream error for {url}: {e}")
        err = b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        writer.write(err)
        await writer.drain()


# ---------------------------------------------------------------------------
# HTTPS tunnel (CONNECT)
# ---------------------------------------------------------------------------

async def handle_connect(host: str, port: int,
                          reader: asyncio.StreamReader,
                          writer: asyncio.StreamWriter):
    """Handle CONNECT tunnel request.

    For accelerated HTTPS domains: we accept the CONNECT, then act as a
    TLS-terminating MITM proxy using the IIISConnect pipeline. This avoids
    the broken upstream CONNECT tunnel while still serving files.

    For other domains: transparent TCP tunnel via upstream proxy.
    """
    bare_host = host.split(":")[0]
    log.info(f"[CONNECT] {bare_host}:{port}")

    if is_accelerated(bare_host):
        # Accept CONNECT — we'll intercept the inner TLS as a MitM proxy.
        # BUT: proper MitM needs a dynamic CA-signed cert per host.
        # Simpler approach: accept CONNECT, then read the TLS ClientHello,
        # realize we can't do MitM without a CA, and instead just try
        # upstream tunnel. If upstream fails, return 502.
        #
        # ACTUALLY: the simplest practical approach for accelerated domains
        # is to NOT use CONNECT at all on the client side. Instead, configure
        # the client to use plain HTTP for those domains.
        #
        # But since we're here (client sent CONNECT), we'll try upstream.
        # If upstream supports it, great. If not, we return 502 immediately
        # instead of hanging.
        pass

    if port == 80:
        # Plain HTTP over CONNECT — we can intercept
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await writer.drain()
        await handle_connect_http_intercept(bare_host, port, reader, writer)
        return

    # Standard: transparent TCP tunnel via upstream proxy
    await tunnel_via_upstream(host, port, reader, writer)


async def handle_connect_http_intercept(host: str, port: int,
                                         reader: asyncio.StreamReader,
                                         writer: asyncio.StreamWriter):
    """After CONNECT established, intercept plain HTTP request."""
    try:
        header_bytes = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)
    except Exception:
        return

    lines = header_bytes.decode(errors="replace").split("\r\n")
    if not lines:
        return
    parts = lines[0].split()
    if len(parts) < 2:
        return
    method, path = parts[0], parts[1]
    url = f"http://{host}{path}"

    headers = []
    for line in lines[1:]:
        if ": " in line:
            k, v = line.split(": ", 1)
            headers.append((k, v))

    await handle_http_request(method, url, "HTTP/1.1", headers, b"", reader, writer)


async def tunnel_via_upstream(host: str, port: int,
                               reader: asyncio.StreamReader,
                               writer: asyncio.StreamWriter):
    """Tunnel TCP through upstream proxy using CONNECT."""
    parsed = urlparse(UPSTREAM_PROXY)
    up_host = parsed.hostname
    up_port = parsed.port or 8080
    try:
        up_reader, up_writer = await asyncio.wait_for(
            asyncio.open_connection(up_host, up_port), timeout=10
        )
        connect_req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
        up_writer.write(connect_req.encode())
        await up_writer.drain()

        # Read upstream's 200 response
        resp_line = await asyncio.wait_for(up_reader.readuntil(b"\r\n\r\n"), timeout=15)
        if b"200" not in resp_line:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            up_writer.close()
            return

        # Tell client tunnel is ready
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await writer.drain()

        # Bidirectional pipe with cancellation
        async def pipe(src, dst, label=""):
            try:
                while True:
                    data = await asyncio.wait_for(src.read(65536), timeout=300)
                    if not data:
                        break
                    dst.write(data)
                    await dst.drain()
            except asyncio.TimeoutError:
                log.debug(f"[TUNNEL] {label} pipe timeout")
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass
            except Exception:
                pass

        t1 = asyncio.create_task(pipe(reader, up_writer, "client→upstream"))
        t2 = asyncio.create_task(pipe(up_reader, writer, "upstream→client"))

        # When either direction finishes, cancel the other
        done, pending = await asyncio.wait(
            [t1, t2], return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        try:
            up_writer.close()
        except Exception:
            pass
    except asyncio.TimeoutError:
        log.warning(f"[TUNNEL] {host}:{port} upstream connect timeout")
        try:
            writer.write(b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        except Exception:
            pass
    except Exception as e:
        log.error(f"[TUNNEL] {host}:{port} failed: {e}")
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main proxy connection handler
# ---------------------------------------------------------------------------

async def handle_proxy_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    try:
        # Read request line + headers
        try:
            header_data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)
        except asyncio.TimeoutError:
            writer.close()
            return
        except Exception:
            writer.close()
            return

        text = header_data.decode(errors="replace")
        lines = text.strip().split("\r\n")
        if not lines:
            writer.close()
            return

        req_line = lines[0].split()
        if len(req_line) < 3:
            writer.close()
            return

        method, target, version = req_line[0], req_line[1], req_line[2]
        headers = []
        body = b""
        content_length = 0

        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                headers.append((k.strip(), v.strip()))
                if k.strip().lower() == "content-length":
                    content_length = int(v.strip())

        if content_length > 0:
            body = await reader.read(content_length)

        if method.upper() == "CONNECT":
            # HTTPS tunnel
            if ":" in target:
                h, p = target.rsplit(":", 1)
                port = int(p)
            else:
                h, port = target, 443
            await handle_connect(h, port, reader, writer)
        else:
            # Plain HTTP proxy
            await handle_http_request(method, target, version, headers, body, reader, writer)

    except Exception as e:
        log.error(f"[PROXY] {peer}: {e}")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def run_proxy():
    server = await asyncio.start_server(
        handle_proxy_client,
        PROXY_HOST,
        PROXY_PORT,
    )
    log.info(f"HTTP proxy listening on {PROXY_HOST}:{PROXY_PORT}")
    log.info(f"Upstream proxy: {UPSTREAM_PROXY}")
    log.info(f"Accelerated domains: {', '.join(sorted(ACCELERATED_DOMAINS))}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_proxy())

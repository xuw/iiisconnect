"""
IIISConnect HTTP/HTTPS Forward Proxy — port 3128

Replaces the slow 192.168.3.226:7890 proxy entirely.
ALL traffic routes through the IIISConnect agent on the iiis cluster!

Routing:
  - Accelerated domains -> Gateway /fetch (Pipeline mode, caching)
  - Other domains -> TCP tunnel through Agent (via WebSocket)

HTTPS MITM for accelerated domains:
  When a client sends CONNECT to an accelerated domain (huggingface.co, etc.),
  we perform TLS MITM: accept the CONNECT, wrap in server-side TLS with a
  self-signed cert, parse the plaintext HTTP requests, and route them through
  the gateway's /fetch API (pipeline + caching). This avoids the SNI mismatch
  problem with TUNNEL_REWRITE for HTTPS and gives full pipeline speed.

  Clients must set REQUESTS_CA_BUNDLE=/data/iiisconnect-ca.pem (or disable
  cert verification) to trust the MITM CA.
"""

import asyncio
import datetime
import logging
import os
import ssl
import tempfile
from urllib.parse import urlparse

import httpx
import websockets

# ---------------------------------------------------------------------------
# MITM CA and certificate generation
# ---------------------------------------------------------------------------
from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

CA_CERT_PATH = os.getenv("IIISCONNECT_CA_CERT_PATH", "/data/iiisconnect-ca.pem")
CA_KEY_PATH = os.getenv("IIISCONNECT_CA_KEY_PATH", "/data/iiisconnect-ca-key.pem")


def _load_or_generate_ca():
    """Load existing CA from disk, or generate a new one.

    Persists to CA_CERT_PATH / CA_KEY_PATH so it survives restarts and other
    pods on the same PVC can trust it.
    """
    if os.path.exists(CA_CERT_PATH) and os.path.exists(CA_KEY_PATH):
        try:
            with open(CA_KEY_PATH, "rb") as f:
                ca_key = serialization.load_pem_private_key(f.read(), password=None)
            with open(CA_CERT_PATH, "rb") as f:
                ca_cert = x509.load_pem_x509_certificate(f.read())
            # Check it hasn't expired
            if ca_cert.not_valid_after_utc > datetime.datetime.now(datetime.timezone.utc):
                return ca_key, ca_cert
        except Exception:
            pass  # Regenerate on any error

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "IIISConnect Proxy CA"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "IIISConnect"),
    ])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    # Persist to disk
    try:
        os.makedirs(os.path.dirname(CA_CERT_PATH) or ".", exist_ok=True)
        with open(CA_KEY_PATH, "wb") as f:
            f.write(ca_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            ))
        os.chmod(CA_KEY_PATH, 0o600)
        with open(CA_CERT_PATH, "wb") as f:
            f.write(ca_cert.public_bytes(serialization.Encoding.PEM))
    except Exception as e:
        logging.getLogger("proxy").warning(f"Could not persist CA to disk: {e}")

    return ca_key, ca_cert


# Module-level CA — generated once at import time
_CA_KEY, _CA_CERT = _load_or_generate_ca()

# Cache of (ssl_context) keyed by domain — avoids regenerating certs
_DOMAIN_SSL_CTX_CACHE: dict[str, ssl.SSLContext] = {}


def _generate_domain_cert(domain: str):
    """Generate a TLS certificate for *domain* signed by the MITM CA.

    Returns an ssl.SSLContext configured as a TLS *server* with the cert.
    """
    if domain in _DOMAIN_SSL_CTX_CACHE:
        return _DOMAIN_SSL_CTX_CACHE[domain]

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, domain),
    ])
    san = x509.SubjectAlternativeName([x509.DNSName(domain)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(_CA_CERT.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365))
        .add_extension(san, critical=False)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .sign(_CA_KEY, hashes.SHA256())
    )

    # Build an SSLContext for server-side TLS
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # Write cert+key to temp files (ssl module needs file paths or in-memory via load)
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    # Use tempfiles that we keep alive — SSLContext loads from them
    cert_file = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)
    cert_file.write(cert_pem + _CA_CERT.public_bytes(serialization.Encoding.PEM))
    cert_file.flush()
    key_file = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)
    key_file.write(key_pem)
    key_file.flush()

    ctx.load_cert_chain(cert_file.name, key_file.name)
    # Clean up temp files (SSLContext has loaded them into memory)
    os.unlink(cert_file.name)
    os.unlink(key_file.name)
    cert_file.close()
    key_file.close()

    _DOMAIN_SSL_CTX_CACHE[domain] = ctx
    return ctx

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

# Tunnel rewrite: blocked foreign domains → accessible China mirrors.
# Applied to both CONNECT tunnels and plain HTTP tunnels.
# For CONNECT (HTTPS): agent connects to mirror; client TLS SNI may mismatch
#   but many CDN mirrors accept any SNI.
# For HTTP tunnels: URL host is rewritten so agent fetches from mirror.
TUNNEL_REWRITE = {
    "huggingface.co": "hf-mirror.com",
    "cdn-lfs.huggingface.co": "hf-mirror.com",
    "cdn-lfs-us-1.huggingface.co": "hf-mirror.com",
    "cdn-lfs-eu-1.huggingface.co": "hf-mirror.com",
}

def is_accelerated(host: str) -> bool:
    host = host.split(":")[0].lower()
    return any(host == d or host.endswith("." + d) for d in ACCELERATED_DOMAINS)

async def _pipe_streams(reader, writer):
    """Pipe data between two streams (TCP ↔ TCP for direct mirror connections)."""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def open_agent_tunnel(host: str, port: int):
    ws_url = f"{GATEWAY_WS}/ws/tunnel_req/{host}/{port}"
    return await websockets.connect(
        ws_url,
        max_size=20*1024*1024,
        ping_interval=None,
        ping_timeout=None
    )

async def handle_connect(host: str, port: int, reader, writer):
    # For accelerated domains, use MITM to intercept HTTPS and route
    # requests through the gateway /fetch API (pipeline + caching).
    if is_accelerated(host) and port == 443:
        await _handle_connect_mitm(host, port, reader, writer)
        return

    # For non-accelerated domains, tunnel through agent as before.
    original_host = host
    rewritten = TUNNEL_REWRITE.get(host.lower())
    if rewritten:
        log.info(f"[CONNECT] {host}:{port} → rewrite to {rewritten}:{port} via Agent")
        host = rewritten

    log.info(f"[CONNECT] {host}:{port} via Agent Tunnel" +
             (f" (was {original_host})" if rewritten else ""))
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


async def _handle_connect_mitm(host: str, port: int, reader, writer):
    """MITM HTTPS CONNECT for accelerated domains.

    Instead of tunneling raw TLS (which causes SNI mismatch when rewriting
    domains), we:
    1. Accept CONNECT and send 200
    2. Wrap the connection in server-side TLS with a cert for the domain
    3. Parse plaintext HTTP requests from the decrypted stream
    4. Route each request through gateway /fetch (pipeline + cache)
    5. Send HTTP responses back over TLS
    """
    log.info(f"[CONNECT-MITM] {host}:{port} — intercepting HTTPS for pipeline acceleration")

    # Generate (or retrieve cached) TLS context for this domain
    try:
        ssl_ctx = _generate_domain_cert(host)
    except Exception as e:
        log.error(f"[CONNECT-MITM] cert generation failed for {host}: {e}")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        await writer.drain()
        return

    # Tell client the tunnel is established
    writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
    await writer.drain()

    # Upgrade the connection to TLS (we are the TLS server)
    loop = asyncio.get_event_loop()
    transport = writer.transport
    try:
        tls_transport = await loop.start_tls(
            transport, transport.get_protocol(), ssl_ctx, server_side=True
        )
    except Exception as e:
        log.error(f"[CONNECT-MITM] TLS handshake failed for {host}: {e}")
        return

    # Replace reader/writer with TLS-wrapped versions
    tls_reader = asyncio.StreamReader()
    tls_protocol = asyncio.StreamReaderProtocol(tls_reader)
    tls_transport.set_protocol(tls_protocol)
    tls_protocol.connection_made(tls_transport)
    tls_writer = asyncio.StreamWriter(tls_transport, tls_protocol, tls_reader, loop)

    # Now read plaintext HTTP requests from the TLS stream
    try:
        await _serve_mitm_requests(host, tls_reader, tls_writer)
    except Exception as e:
        log.debug(f"[CONNECT-MITM] session ended for {host}: {e}")
    finally:
        try:
            tls_writer.close()
        except Exception:
            pass


async def _serve_mitm_requests(host: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Parse and serve HTTP requests over a decrypted TLS connection.

    Supports HTTP/1.1 keep-alive: loops until the client closes or
    sends Connection: close.
    """
    while True:
        # Read request line + headers
        try:
            header_data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=120)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            break

        text = header_data.decode(errors="replace")
        lines = text.strip().split("\r\n")
        if not lines:
            break

        req_parts = lines[0].split()
        if len(req_parts) < 3:
            break
        method, path, version = req_parts[0], req_parts[1], req_parts[2]

        # Parse headers
        headers = {}
        content_length = 0
        connection = "keep-alive"
        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                k_lower = k.strip().lower()
                headers[k_lower] = v.strip()
                if k_lower == "content-length":
                    content_length = int(v.strip())
                elif k_lower == "connection":
                    connection = v.strip().lower()

        # Read request body if present
        body = b""
        if content_length > 0:
            body = await reader.readexactly(content_length)

        # Reconstruct the full URL
        url = f"https://{host}{path}"
        log.info(f"[CONNECT-MITM] {method} {url}")

        # Route GET/HEAD through gateway for accelerated domains
        if method.upper() in ("GET", "HEAD"):
            try:
                await _mitm_fetch_via_gateway(url, method, headers, writer)
                if connection == "close":
                    break
                continue
            except Exception as e:
                log.warning(f"[CONNECT-MITM] gateway fetch failed for {url}: {e}, "
                           f"falling back to agent tunnel")

        # For other methods or if gateway fetch failed, proxy via HTTP to mirror
        await _mitm_proxy_via_http(host, method, path, version, headers, body, writer)
        if connection == "close":
            break


async def _mitm_fetch_via_gateway(url: str, method: str, headers: dict, writer: asyncio.StreamWriter):
    """Fetch a URL via the gateway /fetch API and write the HTTP response.
    
    For HEAD requests, we do the same fetch but only return headers (no body).
    For GET requests to accelerated domains, uses the pipeline + cache path.
    For other URLs, falls back to direct HTTP fetch via the mirror.
    """
    is_head = method.upper() == "HEAD"
    
    # For HEAD requests, we can't use /fetch (it streams the body).
    # Instead, do a direct HTTP request to the mirror URL via the agent tunnel,
    # or use httpx to the mirror directly (agent/iiis can reach hf-mirror).
    # Actually, the simplest: rewrite to HTTP mirror URL and do HEAD there.
    from urllib.parse import urlparse
    parsed = urlparse(url)
    mirror_host = TUNNEL_REWRITE.get(parsed.hostname, parsed.hostname)
    mirror_url = f"http://{mirror_host}{parsed.path}"
    if parsed.query:
        mirror_url += f"?{parsed.query}"
    
    if is_head:
        # HEAD request: proxy through agent tunnel via HTTP to the mirror.
        # We open a TCP tunnel to mirror_host:80 via the agent, send the
        # HEAD request as plain HTTP, and forward the response back.
        try:
            ws = await asyncio.wait_for(open_agent_tunnel(mirror_host, 80), timeout=15)
        except Exception as e:
            raise Exception(f"Agent tunnel for HEAD failed: {e}")

        # Build HTTP HEAD request
        head_path = parsed.path or "/"
        if parsed.query:
            head_path += f"?{parsed.query}"
        head_req = f"HEAD {head_path} HTTP/1.1\r\nHost: {mirror_host}\r\n"
        # Forward important request headers
        for h in ("accept", "user-agent", "if-none-match", "if-modified-since",
                   "authorization"):
            if h in headers:
                head_req += f"{h}: {headers[h]}\r\n"
        head_req += "Connection: close\r\n\r\n"
        
        await ws.send(head_req.encode())
        
        # Read the full response from the WebSocket
        response_data = b""
        try:
            async for msg in ws:
                if isinstance(msg, bytes):
                    response_data += msg
                else:
                    response_data += msg.encode()
        except Exception:
            pass
        
        try:
            await ws.close()
        except Exception:
            pass

        if not response_data:
            raise Exception("Empty response from agent tunnel for HEAD")
        
        # Forward the raw HTTP response to the client
        # But rewrite any Location headers that point to the mirror back to the original
        resp_text = response_data.decode(errors="replace")
        # Replace mirror host references back to original in Location headers
        resp_text = resp_text.replace(f"http://{mirror_host}", f"https://{parsed.hostname}")
        resp_text = resp_text.replace(f"https://{mirror_host}", f"https://{parsed.hostname}")
        
        writer.write(resp_text.encode())
        await writer.drain()
        log.info(f"[CONNECT-MITM] HEAD {url} proxied via {mirror_host}")
        return

    # GET request: use gateway /fetch for pipeline + cache
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(1800, connect=30),
        follow_redirects=True,
    ) as client:
        gw_headers = {}
        for h in ("range", "if-none-match", "if-modified-since", "accept", "accept-encoding"):
            if h in headers:
                gw_headers[h] = headers[h]

        async with client.stream(
            "GET", f"{GATEWAY_API}/fetch", params={"url": url}, headers=gw_headers,
        ) as gw_resp:
            if gw_resp.status_code >= 500:
                raise Exception(f"Gateway error {gw_resp.status_code}")

            status_code = gw_resp.status_code
            status_text = {200: "OK", 206: "Partial Content", 304: "Not Modified",
                          404: "Not Found"}.get(status_code, "OK")

            resp_line = f"HTTP/1.1 {status_code} {status_text}\r\n"
            resp_headers = ""

            if "content-length" in gw_resp.headers:
                resp_headers += f"Content-Length: {gw_resp.headers['content-length']}\r\n"
            content_type = gw_resp.headers.get("content-type", "application/octet-stream")
            resp_headers += f"Content-Type: {content_type}\r\n"
            if "content-disposition" in gw_resp.headers:
                resp_headers += f"Content-Disposition: {gw_resp.headers['content-disposition']}\r\n"
            resp_headers += "Connection: keep-alive\r\n"
            resp_headers += "\r\n"

            writer.write((resp_line + resp_headers).encode())
            await writer.drain()

            total_sent = 0
            async for chunk in gw_resp.aiter_bytes(65536):
                writer.write(chunk)
                await writer.drain()
                total_sent += len(chunk)

            log.info(f"[CONNECT-MITM] Delivered {total_sent / 1e6:.1f} MB for {url}")


async def _mitm_proxy_via_http(host, method, path, version, headers, body, writer):
    """Proxy non-GET/HEAD requests to the HTTP mirror directly.
    
    For methods like POST, PUT, DELETE, etc., we rewrite the URL to the
    HTTP mirror and proxy the request/response. This handles API calls
    that huggingface_hub might make.
    """
    mirror_host = TUNNEL_REWRITE.get(host.lower(), host)
    url = f"http://{mirror_host}{path}"
    log.info(f"[CONNECT-MITM-PROXY] {method} {url} (was {host})")

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60, connect=15),
            follow_redirects=True,
        ) as client:
            # Forward the request to the mirror over HTTP
            req_headers = {}
            for h in ("content-type", "accept", "authorization", "user-agent"):
                if h in headers:
                    req_headers[h] = headers[h]
            req_headers["Host"] = mirror_host

            resp = await client.request(
                method, url,
                headers=req_headers,
                content=body if body else None,
            )

            status_text = {200: "OK", 201: "Created", 204: "No Content",
                          301: "Moved Permanently", 302: "Found", 304: "Not Modified",
                          400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
                          404: "Not Found", 405: "Method Not Allowed",
                          500: "Internal Server Error"}.get(resp.status_code, "OK")

            resp_line = f"HTTP/1.1 {resp.status_code} {status_text}\r\n"
            resp_hdrs = ""
            for h in ("content-type", "content-length", "location", "etag",
                      "x-linked-etag", "x-linked-size", "x-repo-commit",
                      "x-error-code", "x-error-message"):
                if h in resp.headers:
                    resp_hdrs += f"{h}: {resp.headers[h]}\r\n"
            if "content-length" not in resp.headers:
                resp_hdrs += f"Content-Length: {len(resp.content)}\r\n"
            resp_hdrs += "Connection: keep-alive\r\n"
            resp_hdrs += "\r\n"

            writer.write((resp_line + resp_hdrs).encode())
            writer.write(resp.content)
            await writer.drain()
            log.info(f"[CONNECT-MITM-PROXY] {method} {url} → {resp.status_code}")

    except Exception as e:
        log.error(f"[CONNECT-MITM-PROXY] {method} {url} failed: {e}")
        error_body = f"IIISConnect proxy error: {e}"
        error_resp = (
            f"HTTP/1.1 502 Bad Gateway\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(error_body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
            f"{error_body}"
        )
        writer.write(error_resp.encode())
        await writer.drain()

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

    # Tunnel plain HTTP via agent — rewrite blocked domains to mirrors
    port = parsed.port or 80
    original_host = host
    rewritten_host = TUNNEL_REWRITE.get(host.lower())
    if rewritten_host:
        log.info(f"[HTTP-TUNNEL] {host}:{port} → rewrite to {rewritten_host}:{port}")
        host = rewritten_host

    log.info(f"[HTTP-TUNNEL] {host}:{port} via Agent Tunnel" +
             (f" (was {original_host})" if rewritten_host else ""))
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
        hdr_name = k.lower()
        if hdr_name in ('proxy-connection',):
            continue
        # Rewrite Host header to match the mirror
        if hdr_name == 'host' and rewritten_host:
            req += f"{k}: {rewritten_host}\r\n"
        else:
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

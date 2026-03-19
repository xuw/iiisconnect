"""
IIISConnect Agent — runs on iiis cluster.
Connects to xsx gateway via WebSocket, downloads files via smart routing, 
and transfers them using parallel chunked WebSocket streams.
"""
import asyncio
import hashlib
import json
import logging
import os
import struct
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import aiofiles
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("agent")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GATEWAY_WS_URL = os.getenv(
    "IIISCONNECT_GATEWAY_WS",
    "wss://iiisconnect.iiis.co:7443/ws/agent"
)
GATEWAY_DATA_WS_BASE = os.getenv(
    "IIISCONNECT_GATEWAY_DATA_WS",
    "wss://iiisconnect.iiis.co:7443/ws/data"
)
CACHE_DIR = Path(os.getenv("IIISCONNECT_CACHE_DIR", "/cache/iiisconnect"))
CACHE_MAX_BYTES = int(os.getenv("IIISCONNECT_CACHE_MAX_GB", "5000")) * (1024 ** 3)
NUM_DATA_CHANNELS = int(os.getenv("IIISCONNECT_DATA_CHANNELS", "8"))
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB
HEADER_SIZE = 48
HEARTBEAT_INTERVAL = 10  # seconds
RECONNECT_DELAY = 5  # seconds
AGENT_ID = os.getenv("IIISCONNECT_AGENT_ID", "iiis-01")

# ---------------------------------------------------------------------------
# Mirror rules
# ---------------------------------------------------------------------------
MIRROR_RULES = [
    {
        "name": "huggingface",
        "match": lambda url: "huggingface.co" in url,
        "transform": lambda url: url.replace("huggingface.co", "hf-mirror.com"),
        "expected_speed": "9.2 MB/s",
    },
    {
        "name": "github-releases",
        "match": lambda url: "github.com" in url and ("/releases/" in url or "/archive/" in url),
        "transform": lambda url: url.replace("github.com", "mirror.ghproxy.com/https://github.com") 
                                     if "ghproxy" not in url else url,
        "expected_speed": "2 MB/s",
    },
    {
        "name": "pypi",
        "match": lambda url: "pypi.org" in url or "files.pythonhosted.org" in url,
        "transform": lambda url: url.replace("pypi.org", "pypi.tuna.tsinghua.edu.cn")
                                     .replace("files.pythonhosted.org", "pypi.tuna.tsinghua.edu.cn"),
        "expected_speed": "2.3 MB/s",
    },
    {
        "name": "npm",
        "match": lambda url: "registry.npmjs.org" in url,
        "transform": lambda url: url.replace("registry.npmjs.org", "registry.npmmirror.com"),
        "expected_speed": "0.7 MB/s",
    },
    {
        "name": "ubuntu-apt",
        "match": lambda url: "archive.ubuntu.com" in url or "security.ubuntu.com" in url,
        "transform": lambda url: url.replace("archive.ubuntu.com", "mirrors.tuna.tsinghua.edu.cn")
                                     .replace("security.ubuntu.com", "mirrors.tuna.tsinghua.edu.cn"),
        "expected_speed": "93 MB/s",
    },
]


def find_mirror(url: str) -> Optional[dict]:
    """Find best mirror for URL."""
    for rule in MIRROR_RULES:
        if rule["match"](url):
            return rule
    return None


def url_to_hash(url: str) -> str:
    """SHA256 of URL with temporary tokens stripped."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    for k in list(params.keys()):
        kl = k.lower()
        if any(t in kl for t in ("token", "sig", "signature", "expires", "x-amz", "sv=")):
            del params[k]
    clean = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))
    return hashlib.sha256(clean.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
cache_index: Dict[str, dict] = {}


def _cache_meta_path() -> Path:
    return CACHE_DIR / "metadata.json"


def _cache_files_dir() -> Path:
    return CACHE_DIR / "files"


def load_cache_index():
    meta_path = _cache_meta_path()
    global cache_index
    if meta_path.exists():
        try:
            cache_index = json.loads(meta_path.read_text())
            log.info(f"Loaded cache index: {len(cache_index)} entries")
            return
        except Exception as e:
            log.warning(f"Failed to load cache: {e}")

    # Rebuild
    files_dir = _cache_files_dir()
    cache_index = {}
    if not files_dir.exists():
        return
    count = 0
    for entry in files_dir.iterdir():
        if entry.is_dir():
            for f in entry.iterdir():
                if f.is_file() and not f.name.endswith(".meta"):
                    meta_file = f.with_suffix(f.suffix + ".meta")
                    if meta_file.exists():
                        try:
                            meta = json.loads(meta_file.read_text())
                            cache_index[meta.get("hash", entry.name)] = meta
                            count += 1
                        except Exception:
                            pass
    log.info(f"Rebuilt cache: {count} entries")
    save_cache_index()


def save_cache_index():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_meta_path().write_text(json.dumps(cache_index, indent=2))


def cache_lookup(url: str) -> Optional[Path]:
    h = url_to_hash(url)
    entry = cache_index.get(h)
    if entry:
        p = Path(entry["path"])
        if p.exists():
            entry["timestamp"] = time.time()
            return p
        else:
            del cache_index[h]
    return None


def cache_store_file(url: str, filename: str, src_path: Path, size: int, etag: str = ""):
    h = url_to_hash(url)
    dest_dir = _cache_files_dir() / h[:8]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if src_path != dest:
        if dest.exists():
            dest.unlink()
        os.rename(str(src_path), str(dest))

    cache_index[h] = {
        "hash": h,
        "url": url,
        "filename": filename,
        "size": size,
        "timestamp": time.time(),
        "etag": etag,
        "path": str(dest),
    }
    meta_file = dest.with_suffix(dest.suffix + ".meta")
    meta_file.write_text(json.dumps(cache_index[h], indent=2))
    save_cache_index()
    cache_evict()


def cache_evict():
    global cache_index
    total = sum(e.get("size", 0) for e in cache_index.values())
    if total <= CACHE_MAX_BYTES:
        return
    sorted_entries = sorted(cache_index.values(), key=lambda e: e.get("timestamp", 0))
    evicted = 0
    for entry in sorted_entries:
        if total <= CACHE_MAX_BYTES:
            break
        p = Path(entry["path"])
        if p.exists():
            p.unlink()
        total -= entry.get("size", 0)
        del cache_index[entry["hash"]]
        evicted += 1
    if evicted:
        log.info(f"Evicted {evicted} cache entries")
        save_cache_index()


# ---------------------------------------------------------------------------
# Smart download
# ---------------------------------------------------------------------------
async def download_file(url: str, task_id: str, control_ws) -> Optional[Path]:
    """Download file using smart routing. Returns local path or None."""

    # 1. Check local cache
    cached = cache_lookup(url)
    if cached:
        log.info(f"Task {task_id}: cache hit → {cached}")
        await control_ws.send(json.dumps({
            "type": "progress",
            "task_id": task_id,
            "status": "downloading",
            "progress": 100,
            "speed": 0,
            "source": "agent-cache",
        }))
        return cached

    # 2. Try mirror
    mirror = find_mirror(url)
    source = "direct"
    download_url = url
    if mirror:
        download_url = mirror["transform"](url)
        source = f"china-mirror:{mirror['name']}"
        log.info(f"Task {task_id}: using mirror {mirror['name']} → {download_url}")

    # 3. Download
    tmp_dir = CACHE_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Extract filename from URL
    parsed = urlparse(url)
    filename = Path(parsed.path).name or "download"
    tmp_path = tmp_dir / f"{task_id}_{filename}"

    total_size = 0
    downloaded = 0
    start_time = time.time()
    last_report = start_time

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=300.0), follow_redirects=True) as client:
            async with client.stream("GET", download_url) as resp:
                if resp.status_code >= 400:
                    # Fallback to direct if mirror failed
                    if download_url != url:
                        log.warning(f"Task {task_id}: mirror failed ({resp.status_code}), trying direct")
                        source = "direct"
                        download_url = url

                        async with client.stream("GET", download_url) as resp2:
                            if resp2.status_code >= 400:
                                await control_ws.send(json.dumps({
                                    "type": "error",
                                    "task_id": task_id,
                                    "error": f"Download failed: HTTP {resp2.status_code}",
                                }))
                                return None
                            total_size = int(resp2.headers.get("content-length", 0))
                            async with aiofiles.open(tmp_path, "wb") as f:
                                async for chunk in resp2.aiter_bytes(chunk_size=1024 * 1024):
                                    await f.write(chunk)
                                    downloaded += len(chunk)
                                    now = time.time()
                                    if now - last_report >= 2:
                                        elapsed = now - start_time
                                        speed = downloaded / elapsed if elapsed > 0 else 0
                                        progress = int(downloaded * 50 / total_size) if total_size > 0 else 0
                                        await control_ws.send(json.dumps({
                                            "type": "progress",
                                            "task_id": task_id,
                                            "status": "downloading",
                                            "progress": progress,
                                            "speed": int(speed),
                                            "source": source,
                                        }))
                                        last_report = now
                    else:
                        await control_ws.send(json.dumps({
                            "type": "error",
                            "task_id": task_id,
                            "error": f"Download failed: HTTP {resp.status_code}",
                        }))
                        return None
                else:
                    total_size = int(resp.headers.get("content-length", 0))
                    etag = resp.headers.get("etag", "")
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            await f.write(chunk)
                            downloaded += len(chunk)
                            now = time.time()
                            if now - last_report >= 2:
                                elapsed = now - start_time
                                speed = downloaded / elapsed if elapsed > 0 else 0
                                progress = int(downloaded * 50 / total_size) if total_size > 0 else 0
                                await control_ws.send(json.dumps({
                                    "type": "progress",
                                    "task_id": task_id,
                                    "status": "downloading",
                                    "progress": progress,
                                    "speed": int(speed),
                                    "source": source,
                                }))
                                last_report = now

    except Exception as e:
        log.error(f"Task {task_id}: download error: {e}")
        # Fallback to direct if mirror failed
        if download_url != url:
            log.info(f"Task {task_id}: mirror failed, falling back to direct")
            source = "direct-fallback"
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=300.0), follow_redirects=True) as client:
                    async with client.stream("GET", url) as resp:
                        if resp.status_code >= 400:
                            await control_ws.send(json.dumps({
                                "type": "error",
                                "task_id": task_id,
                                "error": f"Direct fallback also failed: HTTP {resp.status_code}",
                            }))
                            return None
                        total_size = int(resp.headers.get("content-length", 0))
                        downloaded = 0
                        start_time = time.time()
                        async with aiofiles.open(tmp_path, "wb") as f:
                            async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                                await f.write(chunk)
                                downloaded += len(chunk)
                                now = time.time()
                                if now - last_report >= 2:
                                    elapsed = now - start_time
                                    speed = downloaded / elapsed if elapsed > 0 else 0
                                    progress = int(downloaded * 50 / total_size) if total_size > 0 else 0
                                    await control_ws.send(json.dumps({
                                        "type": "progress",
                                        "task_id": task_id,
                                        "status": "downloading",
                                        "progress": progress,
                                        "speed": int(speed),
                                        "source": source,
                                    }))
                                    last_report = now
            except Exception as e2:
                await control_ws.send(json.dumps({
                    "type": "error",
                    "task_id": task_id,
                    "error": f"All download attempts failed: {e2}",
                }))
                return None
        else:
            await control_ws.send(json.dumps({
                "type": "error",
                "task_id": task_id,
                "error": f"Download failed: {e}",
            }))
            return None

    elapsed = time.time() - start_time
    actual_size = tmp_path.stat().st_size if tmp_path.exists() else downloaded
    speed = actual_size / elapsed if elapsed > 0 else 0
    log.info(f"Task {task_id}: downloaded {actual_size / 1e6:.1f} MB in {elapsed:.1f}s ({speed / 1e6:.1f} MB/s) via {source}")

    # Store in cache
    cache_store_file(url, filename, tmp_path, actual_size)
    return cache_lookup(url)


# ---------------------------------------------------------------------------
# Chunked transfer via parallel data channels
# ---------------------------------------------------------------------------
async def transfer_file(file_path: Path, task_id: str, control_ws, data_websockets: list):
    """Transfer file to gateway using parallel data channels."""
    file_size = file_path.stat().st_size
    filename = file_path.name
    total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

    # Generate a UUID for the task (used in binary headers)
    task_uuid = uuid.uuid5(uuid.NAMESPACE_URL, task_id).bytes

    # Notify gateway
    await control_ws.send(json.dumps({
        "type": "transfer_start",
        "task_id": task_id,
        "filename": filename,
        "size": file_size,
        "total_chunks": total_chunks,
        "sha256": "",  # Could compute but expensive for large files
    }))

    # Create chunk work queue
    chunk_queue: asyncio.Queue = asyncio.Queue()
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        size = min(CHUNK_SIZE, file_size - offset)
        chunk_queue.put_nowait((i, offset, size))

    transferred = 0
    start_time = time.time()
    lock = asyncio.Lock()

    async def worker(ws, worker_id: int):
        nonlocal transferred
        while True:
            try:
                chunk_idx, offset, size = chunk_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # Read chunk from file
            async with aiofiles.open(file_path, "rb") as f:
                await f.seek(offset)
                data = await f.read(size)

            # Build header (48 bytes)
            header = bytearray(HEADER_SIZE)
            header[0:16] = task_uuid
            struct.pack_into(">Q", header, 16, offset)
            struct.pack_into(">I", header, 24, len(data))
            struct.pack_into(">Q", header, 28, file_size)
            struct.pack_into(">I", header, 36, chunk_idx)
            struct.pack_into(">I", header, 40, total_chunks)
            struct.pack_into(">I", header, 44, 0)  # flags

            frame = bytes(header) + data

            try:
                await ws.send(frame)
                async with lock:
                    transferred += len(data)
                    now = time.time()
                    elapsed = now - start_time
                    speed = transferred / elapsed if elapsed > 0 else 0
                    progress = 50 + int(transferred * 50 / file_size)

                    # Report progress every few chunks
                    if chunk_idx % max(1, total_chunks // 20) == 0 or chunk_idx == total_chunks - 1:
                        await control_ws.send(json.dumps({
                            "type": "progress",
                            "task_id": task_id,
                            "status": "transferring",
                            "progress": min(progress, 99),
                            "speed": int(speed),
                            "source": "websocket",
                        }))
            except Exception as e:
                log.error(f"Worker {worker_id} send error: {e}")
                # Re-queue the chunk
                await chunk_queue.put((chunk_idx, offset, size))
                break

    # Run workers in parallel
    active_ws = [ws for ws in data_websockets if ws is not None]
    if not active_ws:
        log.error(f"Task {task_id}: no data channels available!")
        await control_ws.send(json.dumps({
            "type": "error",
            "task_id": task_id,
            "error": "No data channels available",
        }))
        return

    workers = [asyncio.create_task(worker(ws, i)) for i, ws in enumerate(active_ws)]
    await asyncio.gather(*workers)

    elapsed = time.time() - start_time
    speed = file_size / elapsed if elapsed > 0 else 0
    log.info(f"Task {task_id}: transferred {file_size / 1e6:.1f} MB in {elapsed:.1f}s ({speed / 1e6:.1f} MB/s) via {len(active_ws)} channels")

    # Notify completion
    await control_ws.send(json.dumps({
        "type": "transfer_complete",
        "task_id": task_id,
        "sha256_verified": False,
    }))


# ---------------------------------------------------------------------------
# Main agent loop
# ---------------------------------------------------------------------------
async def connect_data_channels(n: int) -> list:
    """Establish N data channel WebSocket connections."""
    import websockets
    channels = []
    for i in range(n):
        channel_id = f"{AGENT_ID}-data-{i}"
        url = f"{GATEWAY_DATA_WS_BASE}/{channel_id}"
        try:
            ws = await websockets.connect(
                url,
                max_size=20 * 1024 * 1024,
                ping_interval=10,
                ping_timeout=20,
            )
            channels.append(ws)
            log.info(f"Data channel {i} connected")
        except Exception as e:
            log.warning(f"Data channel {i} failed: {e}")
            channels.append(None)
    active = sum(1 for c in channels if c is not None)
    log.info(f"Connected {active}/{n} data channels")
    return channels


async def agent_main():
    """Main agent loop with reconnection."""
    import websockets

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / "files").mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / "tmp").mkdir(parents=True, exist_ok=True)
    load_cache_index()

    while True:
        try:
            log.info(f"Connecting to gateway: {GATEWAY_WS_URL}")
            async with websockets.connect(
                GATEWAY_WS_URL,
                max_size=20 * 1024 * 1024,
                ping_interval=10,
                ping_timeout=20,
            ) as control_ws:
                # Register
                await control_ws.send(json.dumps({
                    "type": "register",
                    "agent_id": AGENT_ID,
                    "capabilities": ["mirror", "direct"],
                }))
                log.info("Registered with gateway")

                # Connect data channels
                data_channels = await connect_data_channels(NUM_DATA_CHANNELS)

                # Start heartbeat task
                async def heartbeat():
                    while True:
                        try:
                            await asyncio.sleep(HEARTBEAT_INTERVAL)
                            await control_ws.send(json.dumps({"type": "heartbeat"}))
                        except Exception:
                            break

                hb_task = asyncio.create_task(heartbeat())

                # Process tasks from gateway
                try:
                    async for message in control_ws:
                        try:
                            msg = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        if msg.get("type") == "task":
                            task_id = msg["task_id"]
                            url = msg["url"]
                            log.info(f"Received task {task_id}: {url}")
                            # Handle task in background
                            asyncio.create_task(
                                handle_task(task_id, url, control_ws, data_channels)
                            )
                        elif msg.get("type") == "heartbeat_ack":
                            pass
                        else:
                            log.debug(f"Unknown message: {msg}")
                finally:
                    hb_task.cancel()
                    # Close data channels
                    for dc in data_channels:
                        if dc:
                            await dc.close()

        except Exception as e:
            log.warning(f"Connection lost: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


async def handle_task(task_id: str, url: str, control_ws, data_channels: list):
    """Handle a download + transfer task."""
    try:
        # Download
        file_path = await download_file(url, task_id, control_ws)
        if not file_path:
            return

        # Transfer to gateway
        await transfer_file(file_path, task_id, control_ws, data_channels)

    except Exception as e:
        log.error(f"Task {task_id} failed: {e}")
        try:
            await control_ws.send(json.dumps({
                "type": "error",
                "task_id": task_id,
                "error": str(e),
            }))
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(agent_main())

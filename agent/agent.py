"""
IIISConnect Agent — runs on iiis cluster.
Connects to xsx gateway via WebSocket, downloads files via smart routing,
and transfers them using parallel chunked WebSocket streams.

v2: Pipeline mode — download and transfer overlap. Chunks are queued for
    WebSocket send as soon as they arrive from the HTTP stream, so the
    gateway starts receiving data while the download is still in progress.
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
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB — matches WebSocket frame size
HEADER_SIZE = 48
HEARTBEAT_INTERVAL = 10  # seconds
RECONNECT_DELAY = 5  # seconds
AGENT_ID = os.getenv("IIISCONNECT_AGENT_ID", "iiis-01")

# Pipeline queue depth: how many chunks can sit in memory waiting for send.
# Too large wastes RAM; too small starves the senders.
PIPELINE_QUEUE_DEPTH = int(os.getenv("IIISCONNECT_PIPELINE_DEPTH", "32"))

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
# AWS relay (non-pipeline — downloads to local file first, then pipelines from there)
# ---------------------------------------------------------------------------

async def download_aws_relay(url: str, task_id: str, dest_path: Path, control_ws) -> bool:
    ssh_key = "/data/iiisconnect/xuw-aws-jp-2026.pem"
    ssh_host = "ec2-user@13.208.212.186"

    if not os.path.exists(ssh_key):
        log.error(f"AWS relay failed: SSH key {ssh_key} not found")
        return False

    parsed = urlparse(url)
    filename = Path(parsed.path).name or "download"
    remote_tmp = f"/tmp/{task_id}_{filename}"

    log.info(f"Task {task_id}: Starting AWS relay download to {remote_tmp}")
    await control_ws.send(json.dumps({
        "type": "progress", "task_id": task_id, "status": "downloading",
        "progress": 0, "speed": 0, "source": "aws-relay (remote fetch)"
    }))

    # 1. Download on AWS using curl
    curl_cmd = f"curl -C - -sL -o '{remote_tmp}' '{url}'"
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-i", ssh_key, ssh_host, curl_cmd]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()
    if proc.returncode != 0:
        log.error(f"Task {task_id}: AWS curl failed with code {proc.returncode}")
        return False

    # Get remote size
    size_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-i", ssh_key, ssh_host, f"stat -c %s '{remote_tmp}'"]
    proc_sz = await asyncio.create_subprocess_exec(*size_cmd, stdout=asyncio.subprocess.PIPE)
    out, _ = await proc_sz.communicate()
    try:
        total_size = int(out.decode().strip())
    except:
        total_size = 0

    log.info(f"Task {task_id}: AWS fetch done ({total_size} bytes). Rsyncing back.")

    # 2. Rsync from AWS to local
    rsync_cmd = [
        "rsync", "-a", "--append", "--partial", "-e", f"ssh -o StrictHostKeyChecking=no -i {ssh_key}",
        f"{ssh_host}:{remote_tmp}", str(dest_path)
    ]
    start_time = time.time()

    proc = await asyncio.create_subprocess_exec(*rsync_cmd)

    while proc.returncode is None:
        await asyncio.sleep(2)
        if total_size > 0 and dest_path.exists():
            downloaded = dest_path.stat().st_size
            now = time.time()
            elapsed = now - start_time
            speed = downloaded / elapsed if elapsed > 0 else 0
            progress = int(downloaded * 50 / total_size) if total_size > 0 else 0
            try:
                await control_ws.send(json.dumps({
                    "type": "progress", "task_id": task_id, "status": "downloading",
                    "progress": progress, "speed": int(speed), "source": "aws-relay (rsync)"
                }))
            except:
                pass

    await proc.wait()

    # 3. Clean up remote file
    rm_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-i", ssh_key, ssh_host, f"rm -f '{remote_tmp}'"]
    asyncio.create_task(asyncio.create_subprocess_exec(*rm_cmd))

    return proc.returncode == 0 and dest_path.exists()


# ---------------------------------------------------------------------------
# Pipeline: overlapped download + transfer
# ---------------------------------------------------------------------------

async def _stream_download(
    download_url: str,
    local_file: Path,
    chunk_queue: asyncio.Queue,
    task_id: str,
    control_ws,
    source: str,
) -> int:
    """
    Download *download_url*, writing to *local_file* for cache and
    simultaneously putting (chunk_idx, offset, data_bytes) into *chunk_queue*
    so that the transfer workers can send them over WebSocket in parallel.

    Returns total bytes written.
    """
    total_size = 0
    downloaded = 0
    chunk_idx = 0
    start_time = time.time()
    last_report = start_time

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, read=300.0), follow_redirects=True
    ) as client:
        async with client.stream("GET", download_url) as resp:
            if resp.status_code >= 400:
                raise httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )

            total_size = int(resp.headers.get("content-length", 0))

            async with aiofiles.open(local_file, "wb") as f:
                buf = bytearray()
                async for raw in resp.aiter_bytes(chunk_size=256 * 1024):
                    buf.extend(raw)
                    # Flush full CHUNK_SIZE pieces
                    while len(buf) >= CHUNK_SIZE:
                        piece = bytes(buf[:CHUNK_SIZE])
                        del buf[:CHUNK_SIZE]
                        offset = chunk_idx * CHUNK_SIZE
                        await f.write(piece)
                        downloaded += len(piece)
                        # Put into pipeline queue (may block if senders are slow)
                        await chunk_queue.put((chunk_idx, offset, piece))
                        chunk_idx += 1

                        # Progress
                        now = time.time()
                        if now - last_report >= 2:
                            elapsed = now - start_time
                            speed = downloaded / elapsed if elapsed > 0 else 0
                            pct = int(downloaded * 100 / total_size) if total_size > 0 else 0
                            try:
                                await control_ws.send(json.dumps({
                                    "type": "progress",
                                    "task_id": task_id,
                                    "status": "pipeline",
                                    "progress": pct,
                                    "speed": int(speed),
                                    "source": source,
                                }))
                            except:
                                pass
                            last_report = now

                # Flush remaining bytes in buf
                if buf:
                    piece = bytes(buf)
                    offset = chunk_idx * CHUNK_SIZE
                    await f.write(piece)
                    downloaded += len(piece)
                    await chunk_queue.put((chunk_idx, offset, piece))
                    chunk_idx += 1

    return downloaded


async def _transfer_workers(
    chunk_queue: asyncio.Queue,
    data_websockets: list,
    task_uuid: bytes,
    file_size: int,
    total_chunks_est: int,
    task_id: str,
    control_ws,
    done_event: asyncio.Event,
):
    """
    Consume (chunk_idx, offset, data) from *chunk_queue* and send them over
    parallel WebSocket data channels.  Exits when *done_event* is set AND
    the queue is empty.
    """
    active_ws = [ws for ws in data_websockets if ws is not None]
    if not active_ws:
        log.error(f"Task {task_id}: no data channels for pipeline transfer")
        return

    transferred = 0
    lock = asyncio.Lock()
    start_time = time.time()

    async def worker(ws, wid: int):
        nonlocal transferred
        while True:
            try:
                chunk_idx, offset, data = chunk_queue.get_nowait()
            except asyncio.QueueEmpty:
                if done_event.is_set():
                    return  # producer finished
                await asyncio.sleep(0.01)
                continue

            # Build binary frame
            header = bytearray(HEADER_SIZE)
            header[0:16] = task_uuid
            struct.pack_into(">Q", header, 16, offset)
            struct.pack_into(">I", header, 24, len(data))
            struct.pack_into(">Q", header, 28, file_size)
            struct.pack_into(">I", header, 36, chunk_idx)
            struct.pack_into(">I", header, 40, total_chunks_est)
            struct.pack_into(">I", header, 44, 0)

            frame = bytes(header) + data

            try:
                await ws.send(frame)
                async with lock:
                    transferred += len(data)
            except Exception as e:
                log.error(f"Pipeline worker {wid} send error: {e}")
                # re-queue so another worker can pick it up
                await chunk_queue.put((chunk_idx, offset, data))
                break

    workers = [asyncio.create_task(worker(ws, i)) for i, ws in enumerate(active_ws)]
    await asyncio.gather(*workers)

    elapsed = time.time() - start_time
    speed = transferred / elapsed if elapsed > 0 else 0
    log.info(
        f"Task {task_id}: pipeline transfer sent {transferred / 1e6:.1f} MB "
        f"in {elapsed:.1f}s ({speed / 1e6:.1f} MB/s) via {len(active_ws)} channels"
    )


async def handle_task_pipeline(
    task_id: str,
    url: str,
    control_ws,
    data_channels: list,
):
    """
    Pipeline task handler: download and transfer happen concurrently.
    """
    try:
        # ---- 1. Cache hit? ----
        cached = cache_lookup(url)
        if cached:
            log.info(f"Task {task_id}: cache hit → {cached}")
            await control_ws.send(json.dumps({
                "type": "progress", "task_id": task_id,
                "status": "downloading", "progress": 100,
                "speed": 0, "source": "agent-cache",
            }))
            # Transfer the cached file (non-pipeline, already local)
            await transfer_file_from_disk(cached, task_id, control_ws, data_channels)
            return

        # ---- 2. Resolve download URL ----
        parsed = urlparse(url)
        mirror = find_mirror(url)
        source = "direct"
        download_url = url
        if mirror:
            download_url = mirror["transform"](url)
            source = f"china-mirror:{mirror['name']}"
            log.info(f"Task {task_id}: using mirror {mirror['name']} → {download_url}")

        # ---- 2.5 AWS Relay (non-pipeline, falls through to disk transfer) ----
        is_foreign = False
        if not mirror:
            host = parsed.hostname or ""
            if not (host.endswith(".cn") or "tuna.tsinghua" in host
                    or "mirror" in host or "aliyun" in host):
                is_foreign = True

        if is_foreign and os.path.exists("/data/iiisconnect/xuw-aws-jp-2026.pem"):
            log.info(f"Task {task_id}: Trying aws-relay for {url}")
            tmp_dir = CACHE_DIR / "tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            filename = Path(parsed.path).name or "download"
            tmp_path = tmp_dir / f"{task_id}_{filename}"
            success = await download_aws_relay(url, task_id, tmp_path, control_ws)
            if success:
                actual_size = tmp_path.stat().st_size
                cache_store_file(url, filename, tmp_path, actual_size)
                cached = cache_lookup(url)
                if cached:
                    await transfer_file_from_disk(cached, task_id, control_ws, data_channels)
                return
            log.warning(f"Task {task_id}: aws-relay failed, falling back")

        # ---- 3. Pipeline download + transfer ----
        filename = Path(parsed.path).name or "download"
        tmp_dir = CACHE_DIR / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / f"{task_id}_{filename}"

        # We need to know total_size for the transfer_start message.
        # Do a HEAD request first.
        total_size = 0
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(15.0), follow_redirects=True
            ) as cl:
                head = await cl.head(download_url)
                total_size = int(head.headers.get("content-length", 0))
        except Exception:
            pass  # unknown size is okay, we'll update

        total_chunks_est = max(1, (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE) if total_size > 0 else 0

        task_uuid = uuid.uuid5(uuid.NAMESPACE_URL, task_id).bytes

        # Notify gateway of transfer start
        await control_ws.send(json.dumps({
            "type": "transfer_start",
            "task_id": task_id,
            "filename": filename,
            "size": total_size,
            "total_chunks": total_chunks_est,
            "sha256": "",
        }))

        # Shared bounded queue
        chunk_queue: asyncio.Queue = asyncio.Queue(maxsize=PIPELINE_QUEUE_DEPTH)
        done_event = asyncio.Event()

        # Launch transfer workers
        transfer_task = asyncio.create_task(
            _transfer_workers(
                chunk_queue, data_channels, task_uuid,
                total_size, total_chunks_est,
                task_id, control_ws, done_event,
            )
        )

        # Download (producer) — blocks until complete
        start_time = time.time()
        try:
            actual_size = await _stream_download(
                download_url, tmp_path, chunk_queue,
                task_id, control_ws, source,
            )
        except Exception as dl_err:
            log.error(f"Task {task_id}: pipeline download error: {dl_err}")
            # Try direct fallback if mirror was used
            if download_url != url:
                log.info(f"Task {task_id}: mirror failed, retrying direct in pipeline")
                source = "direct-fallback"
                try:
                    actual_size = await _stream_download(
                        url, tmp_path, chunk_queue,
                        task_id, control_ws, source,
                    )
                except Exception as dl_err2:
                    done_event.set()
                    await transfer_task
                    await control_ws.send(json.dumps({
                        "type": "error", "task_id": task_id,
                        "error": f"All download attempts failed: {dl_err2}",
                    }))
                    return
            else:
                done_event.set()
                await transfer_task
                await control_ws.send(json.dumps({
                    "type": "error", "task_id": task_id,
                    "error": f"Download failed: {dl_err}",
                }))
                return

        # Signal workers that no more chunks are coming
        done_event.set()
        await transfer_task

        elapsed = time.time() - start_time
        speed = actual_size / elapsed if elapsed > 0 else 0
        log.info(
            f"Task {task_id}: pipeline finished {actual_size / 1e6:.1f} MB "
            f"in {elapsed:.1f}s ({speed / 1e6:.1f} MB/s end-to-end) via {source}"
        )

        # Store in cache
        cache_store_file(url, filename, tmp_path, actual_size)

        # Notify gateway: transfer complete
        try:
            await control_ws.send(json.dumps({
                "type": "transfer_complete",
                "task_id": task_id,
                "sha256_verified": False,
            }))
        except Exception as e:
            log.warning(f"Task {task_id}: complete notification failed: {e}")

    except Exception as e:
        log.error(f"Task {task_id} failed: {e}", exc_info=True)
        try:
            await control_ws.send(json.dumps({
                "type": "error", "task_id": task_id, "error": str(e),
            }))
        except:
            pass


# ---------------------------------------------------------------------------
# Legacy disk-based transfer (used for cache-hit & AWS relay)
# ---------------------------------------------------------------------------

async def transfer_file_from_disk(
    file_path: Path, task_id: str, control_ws, data_websockets: list
):
    """Transfer an already-local file to gateway using parallel data channels."""
    file_size = file_path.stat().st_size
    filename = file_path.name
    total_chunks = max(1, (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE)

    task_uuid = uuid.uuid5(uuid.NAMESPACE_URL, task_id).bytes

    # Notify gateway
    await control_ws.send(json.dumps({
        "type": "transfer_start",
        "task_id": task_id,
        "filename": filename,
        "size": file_size,
        "total_chunks": total_chunks,
        "sha256": "",
    }))

    # Build chunk work queue
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

            async with aiofiles.open(file_path, "rb") as f:
                await f.seek(offset)
                data = await f.read(size)

            header = bytearray(HEADER_SIZE)
            header[0:16] = task_uuid
            struct.pack_into(">Q", header, 16, offset)
            struct.pack_into(">I", header, 24, len(data))
            struct.pack_into(">Q", header, 28, file_size)
            struct.pack_into(">I", header, 36, chunk_idx)
            struct.pack_into(">I", header, 40, total_chunks)
            struct.pack_into(">I", header, 44, 0)

            frame = bytes(header) + data

            try:
                await ws.send(frame)
                async with lock:
                    transferred += len(data)
                    if chunk_idx % max(1, total_chunks // 20) == 0 or chunk_idx == total_chunks - 1:
                        now = time.time()
                        elapsed = now - start_time
                        speed = transferred / elapsed if elapsed > 0 else 0
                        progress = int(transferred * 100 / file_size)
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
                await chunk_queue.put((chunk_idx, offset, size))
                break

    active_ws = [ws for ws in data_websockets if ws is not None]
    if not active_ws:
        log.error(f"Task {task_id}: no data channels available!")
        await control_ws.send(json.dumps({
            "type": "error", "task_id": task_id,
            "error": "No data channels available",
        }))
        return

    workers = [asyncio.create_task(worker(ws, i)) for i, ws in enumerate(active_ws)]
    await asyncio.gather(*workers)

    elapsed = time.time() - start_time
    speed = file_size / elapsed if elapsed > 0 else 0
    log.info(
        f"Task {task_id}: disk transfer {file_size / 1e6:.1f} MB "
        f"in {elapsed:.1f}s ({speed / 1e6:.1f} MB/s) via {len(active_ws)} channels"
    )

    try:
        await control_ws.send(json.dumps({
            "type": "transfer_complete",
            "task_id": task_id,
            "sha256_verified": False,
        }))
    except Exception as e:
        log.warning(f"Task {task_id}: complete notification failed: {e}")


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
                ping_interval=30,
                ping_timeout=60,
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
                ping_interval=30,
                ping_timeout=60,
            ) as control_ws:
                # Register
                await control_ws.send(json.dumps({
                    "type": "register",
                    "agent_id": AGENT_ID,
                    "capabilities": ["mirror", "direct", "aws-relay", "pipeline"],
                }))
                log.info("Registered with gateway")

                # Connect data channels
                data_channels = await connect_data_channels(NUM_DATA_CHANNELS)

                # Start heartbeat
                async def heartbeat():
                    while True:
                        try:
                            await asyncio.sleep(HEARTBEAT_INTERVAL)
                            await control_ws.send(json.dumps({"type": "heartbeat"}))
                        except Exception:
                            break

                hb_task = asyncio.create_task(heartbeat())

                # Process tasks
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
                            asyncio.create_task(
                                handle_task_pipeline(task_id, url, control_ws, data_channels)
                            )
                        elif msg.get("type") == "heartbeat_ack":
                            pass
                        else:
                            log.debug(f"Unknown message: {msg}")
                finally:
                    hb_task.cancel()
                    for dc in data_channels:
                        if dc:
                            try:
                                await dc.close()
                            except Exception:
                                pass

            log.warning("Control WebSocket closed cleanly by server.")
            await asyncio.sleep(RECONNECT_DELAY)

        except Exception as e:
            log.warning(f"Connection lost: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    asyncio.run(agent_main())

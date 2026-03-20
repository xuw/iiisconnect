"""
IIISConnect Gateway — runs on xsx cluster.
Accepts WebSocket connections from iiis agent, serves download API to xsx pods.
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
from typing import Dict, Optional, Set

import aiofiles
import aiofiles.os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CACHE_DIR = Path(os.getenv("IIISCONNECT_CACHE_DIR", "/data/iiisconnect-cache"))
CACHE_MAX_BYTES = int(os.getenv("IIISCONNECT_CACHE_MAX_GB", "5000")) * (1024 ** 3)  # 5TB default
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB
HEADER_SIZE = 48
BIND_HOST = os.getenv("IIISCONNECT_HOST", "0.0.0.0")
BIND_PORT = int(os.getenv("IIISCONNECT_PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class TaskState:
    """Tracks a single download task."""
    def __init__(self, task_id: str, url: str, dest: Optional[str] = None):
        self.task_id = task_id
        self.url = url
        self.dest = dest
        self.status = "queued"  # queued | downloading | transferring | completed | failed
        self.progress = 0  # 0-100
        self.speed = 0  # bytes/sec
        self.source = ""
        self.error = ""
        self.filename = ""
        self.total_size = 0
        self.received_bytes = 0
        self.total_chunks = 0
        self.received_chunks: Set[int] = set()
        self.created_at = time.time()
        self.completed_at: Optional[float] = None
        self._file_handle = None
        self._tmp_path: Optional[Path] = None

    def to_dict(self) -> dict:
        d = {
            "task_id": self.task_id,
            "url": self.url,
            "status": self.status,
            "progress": self.progress,
            "speed": self.speed,
            "source": self.source,
            "filename": self.filename,
            "total_size": self.total_size,
            "received_bytes": self.received_bytes,
            "created_at": self.created_at,
        }
        if self.error:
            d["error"] = self.error
        if self.completed_at:
            d["completed_at"] = self.completed_at
            d["duration"] = round(self.completed_at - self.created_at, 1)
        return d


tasks: Dict[str, TaskState] = {}
task_uuid_map: Dict[bytes, str] = {}  # uuid5 bytes -> task_id
agent_ws: Optional[WebSocket] = None
data_channels: Dict[str, WebSocket] = {}  # channel_id -> ws
agent_connected = asyncio.Event()

# Cache metadata: url_hash -> {url, filename, size, timestamp, etag, path}
cache_index: Dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def url_to_hash(url: str) -> str:
    """SHA256 of URL with temporary tokens stripped."""
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    # Remove common temporary parameters
    for k in list(params.keys()):
        kl = k.lower()
        if any(t in kl for t in ("token", "sig", "signature", "expires", "x-amz", "sv=")):
            del params[k]
    clean = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))
    return hashlib.sha256(clean.encode()).hexdigest()


def _cache_meta_path() -> Path:
    return CACHE_DIR / "metadata.json"


def _cache_files_dir() -> Path:
    return CACHE_DIR / "files"


async def load_cache_index():
    """Load or rebuild cache index from disk."""
    global cache_index
    meta_path = _cache_meta_path()
    if meta_path.exists():
        try:
            async with aiofiles.open(meta_path, "r") as f:
                cache_index = json.loads(await f.read())
            log.info(f"Loaded cache index: {len(cache_index)} entries")
            return
        except Exception as e:
            log.warning(f"Failed to load cache index: {e}")

    # Rebuild from files directory
    files_dir = _cache_files_dir()
    if not files_dir.exists():
        cache_index = {}
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
                            h = meta.get("hash", entry.name)
                            cache_index[h] = meta
                            count += 1
                        except Exception:
                            pass
    log.info(f"Rebuilt cache index from disk: {count} entries")
    await save_cache_index()


async def save_cache_index():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(_cache_meta_path(), "w") as f:
        await f.write(json.dumps(cache_index, indent=2))


def cache_lookup(url: str) -> Optional[Path]:
    """Check if URL is cached. Returns file path or None."""
    h = url_to_hash(url)
    entry = cache_index.get(h)
    if entry:
        p = Path(entry["path"])
        if p.exists():
            # Update access timestamp (LRU)
            entry["timestamp"] = time.time()
            return p
        else:
            # Stale entry
            del cache_index[h]
    return None


async def cache_store(url: str, filename: str, file_path: Path, size: int, etag: str = ""):
    """Register a file in cache."""
    h = url_to_hash(url)
    dest_dir = _cache_files_dir() / h[:8]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if file_path != dest:
        # Move file to cache location
        if dest.exists():
            dest.unlink()
        os.rename(str(file_path), str(dest))

    cache_index[h] = {
        "hash": h,
        "url": url,
        "filename": filename,
        "size": size,
        "timestamp": time.time(),
        "etag": etag,
        "path": str(dest),
    }

    # Save per-file metadata
    meta_file = dest.with_suffix(dest.suffix + ".meta")
    async with aiofiles.open(meta_file, "w") as f:
        await f.write(json.dumps(cache_index[h], indent=2))

    await save_cache_index()

    # LRU eviction if needed
    await cache_evict()


async def cache_evict():
    """Evict oldest entries until under CACHE_MAX_BYTES."""
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
            meta_f = p.with_suffix(p.suffix + ".meta")
            if meta_f.exists():
                meta_f.unlink()
        total -= entry.get("size", 0)
        del cache_index[entry["hash"]]
        evicted += 1

    if evicted:
        log.info(f"Cache eviction: removed {evicted} entries")
        await save_cache_index()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="IIISConnect Gateway", version="1.0.0")


@app.on_event("startup")
async def startup():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_files_dir().mkdir(parents=True, exist_ok=True)
    await load_cache_index()
    log.info(f"Gateway started. Cache dir: {CACHE_DIR}")


# ---- Health ----
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "agent_connected": agent_ws is not None,
        "data_channels": len(data_channels),
        "cache_entries": len(cache_index),
        "cache_size_gb": round(sum(e.get("size", 0) for e in cache_index.values()) / (1024**3), 2),
        "active_tasks": sum(1 for t in tasks.values() if t.status not in ("completed", "failed")),
    }


# ---- Download API ----
@app.post("/download")
async def submit_download(body: dict):
    url = body.get("url")
    dest = body.get("dest")
    if not url:
        raise HTTPException(400, "url is required")

    # Check cache first
    cached = cache_lookup(url)
    if cached:
        task_id = str(uuid.uuid4())[:8]
        t = TaskState(task_id, url, dest)
        t.status = "completed"
        t.source = "gateway-cache"
        t.filename = cached.name
        t.total_size = cached.stat().st_size
        t.received_bytes = t.total_size
        t.progress = 100
        t.completed_at = time.time()
        tasks[task_id] = t

        # Copy to dest if specified
        if dest:
            asyncio.create_task(_copy_cached(cached, dest))

        return {"task_id": task_id, "status": "completed", "source": "gateway-cache", "path": str(cached)}

    # Need agent
    if agent_ws is None:
        raise HTTPException(503, "Agent not connected")

    task_id = str(uuid.uuid4())[:8]
    t = TaskState(task_id, url, dest)
    tasks[task_id] = t
    # Pre-compute UUID5 for binary header matching
    task_uuid_bytes = uuid.uuid5(uuid.NAMESPACE_URL, task_id).bytes
    task_uuid_map[task_uuid_bytes] = task_id

    # Send task to agent
    try:
        await agent_ws.send_json({
            "type": "task",
            "task_id": task_id,
            "url": url,
        })
        log.info(f"Task {task_id} sent to agent: {url}")
    except Exception as e:
        t.status = "failed"
        t.error = str(e)
        raise HTTPException(503, f"Failed to send task to agent: {e}")

    return {"task_id": task_id, "status": "queued"}


async def _copy_cached(src: Path, dest_dir: str):
    """Copy cached file to destination."""
    try:
        dest_path = Path(dest_dir)
        dest_path.mkdir(parents=True, exist_ok=True)
        final = dest_path / src.name
        async with aiofiles.open(src, "rb") as rf:
            async with aiofiles.open(final, "wb") as wf:
                while True:
                    chunk = await rf.read(1024 * 1024)
                    if not chunk:
                        break
                    await wf.write(chunk)
        log.info(f"Copied cached file to {final}")
    except Exception as e:
        log.error(f"Failed to copy cached file: {e}")


# ---- Status ----
@app.get("/status/{task_id}")
async def get_status(task_id: str):
    t = tasks.get(task_id)
    if not t:
        raise HTTPException(404, "Task not found")
    return t.to_dict()


# ---- Jobs list ----
@app.get("/jobs")
async def list_jobs(limit: int = Query(50)):
    sorted_tasks = sorted(tasks.values(), key=lambda t: t.created_at, reverse=True)[:limit]
    return [t.to_dict() for t in sorted_tasks]


# ---- Cache list ----
@app.get("/cache")
async def list_cache():
    entries = []
    for h, entry in sorted(cache_index.items(), key=lambda x: x[1].get("timestamp", 0), reverse=True):
        entries.append({
            "hash": h[:16] + "...",
            "url": entry.get("url", ""),
            "filename": entry.get("filename", ""),
            "size_mb": round(entry.get("size", 0) / (1024 * 1024), 1),
            "cached_at": entry.get("timestamp", 0),
        })
    return {
        "total_entries": len(entries),
        "total_size_gb": round(sum(e.get("size", 0) for e in cache_index.values()) / (1024**3), 2),
        "entries": entries,
    }


@app.delete("/cache/{hash_prefix}")
async def delete_cache(hash_prefix: str):
    to_del = [h for h in cache_index if h.startswith(hash_prefix)]
    if not to_del:
        raise HTTPException(404, "Cache entry not found")
    for h in to_del:
        entry = cache_index[h]
        p = Path(entry["path"])
        if p.exists():
            p.unlink()
        del cache_index[h]
    await save_cache_index()
    return {"deleted": len(to_del)}


# ---- Fetch (streaming proxy) ----
@app.get("/fetch")
async def fetch_file(url: str = Query(...)):
    cached = cache_lookup(url)
    if cached:
        async def stream():
            async with aiofiles.open(cached, "rb") as f:
                while True:
                    chunk = await f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
        return StreamingResponse(stream(), media_type="application/octet-stream",
                                 headers={"Content-Disposition": f'attachment; filename="{cached.name}"'})

    # Submit download and wait
    if agent_ws is None:
        raise HTTPException(503, "Agent not connected")

    task_id = str(uuid.uuid4())[:8]
    t = TaskState(task_id, url, None)
    tasks[task_id] = t
    task_uuid_map[uuid.uuid5(uuid.NAMESPACE_URL, task_id).bytes] = task_id

    await agent_ws.send_json({"type": "task", "task_id": task_id, "url": url})

    # Wait for completion (up to 30 minutes)
    for _ in range(1800):
        await asyncio.sleep(1)
        if t.status == "completed":
            cached = cache_lookup(url)
            if cached:
                async def stream():
                    async with aiofiles.open(cached, "rb") as f:
                        while True:
                            chunk = await f.read(1024 * 1024)
                            if not chunk:
                                break
                            yield chunk
                return StreamingResponse(stream(), media_type="application/octet-stream",
                                         headers={"Content-Disposition": f'attachment; filename="{cached.name}"'})
        elif t.status == "failed":
            raise HTTPException(502, f"Download failed: {t.error}")

    raise HTTPException(504, "Download timed out")


# ---------------------------------------------------------------------------
# WebSocket: Agent control channel
# ---------------------------------------------------------------------------
@app.websocket("/ws/agent")
async def ws_agent(ws: WebSocket):
    global agent_ws
    await ws.accept()
    old_ws = agent_ws
    agent_ws = ws
    agent_connected.set()
    log.info("Agent connected (control channel)")

    # Close old connection if any
    if old_ws is not None and old_ws is not ws:
        try:
            await old_ws.close()
        except Exception:
            pass

    try:
        while True:
            msg = await ws.receive_json()
            await handle_agent_message(msg)
    except WebSocketDisconnect:
        log.warning("Agent disconnected")
    except Exception as e:
        log.error(f"Agent WS error: {e}")
    finally:
        # Only clear if we're still the current agent
        if agent_ws is ws:
            agent_ws = None
            agent_connected.clear()


async def handle_agent_message(msg: dict):
    """Handle JSON messages from agent on control channel."""
    msg_type = msg.get("type")
    task_id = msg.get("task_id")

    if msg_type == "register":
        log.info(f"Agent registered: {msg.get('agent_id')} caps={msg.get('capabilities')}")
        return

    if msg_type == "heartbeat":
        if agent_ws:
            await agent_ws.send_json({"type": "heartbeat_ack"})
        return

    if msg_type == "progress":
        t = tasks.get(task_id)
        if t:
            t.status = msg.get("status", t.status)
            t.progress = msg.get("progress", t.progress)
            t.speed = msg.get("speed", t.speed)
            t.source = msg.get("source", t.source)
            log.info(f"Task {task_id}: {t.status} {t.progress}% {t.speed / 1e6:.1f} MB/s via {t.source}")

    elif msg_type == "transfer_start":
        t = tasks.get(task_id)
        if t:
            t.status = "transferring"
            t.filename = msg.get("filename", "")
            t.total_size = msg.get("size", 0)
            t.total_chunks = msg.get("total_chunks", 0)
            t.received_chunks = set()
            t.received_bytes = 0
            t.transfer_started_at = time.time()
            t._pipeline_mode = (t.total_size == 0)  # pipeline mode if size unknown

            # Create temp file
            tmp_dir = CACHE_DIR / "tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            t._tmp_path = tmp_dir / f"{task_id}_{t.filename}"
            if t.total_size > 0:
                # Pre-allocate file for sparse writing
                async with aiofiles.open(t._tmp_path, "wb") as f:
                    await f.seek(t.total_size - 1)
                    await f.write(b"\0")
            else:
                # Pipeline mode: create empty file, will append/seek as chunks arrive
                async with aiofiles.open(t._tmp_path, "wb") as f:
                    pass
            log.info(f"Task {task_id}: transfer starting, {t.total_size / 1e6:.1f} MB, {t.total_chunks} chunks (pipeline={t._pipeline_mode})")

    elif msg_type == "transfer_complete":
        t = tasks.get(task_id)
        if t:
            t.status = "completed"
            t.progress = 100
            t.completed_at = time.time()
            total_duration = t.completed_at - t.created_at
            transfer_duration = t.completed_at - getattr(t, 'transfer_started_at', t.created_at)
            avg_speed = t.total_size / transfer_duration if transfer_duration > 0 else 0
            log.info(f"Task {task_id}: completed in {total_duration:.1f}s (transfer {transfer_duration:.1f}s), transfer speed {avg_speed / 1e6:.1f} MB/s")

            # Move to cache
            if t._tmp_path and t._tmp_path.exists():
                await cache_store(t.url, t.filename, t._tmp_path, t.total_size)

                # Copy to dest if specified
                if t.dest:
                    cached = cache_lookup(t.url)
                    if cached:
                        asyncio.create_task(_copy_cached(cached, t.dest))

    elif msg_type == "error":
        t = tasks.get(task_id)
        if t:
            t.status = "failed"
            t.error = msg.get("error", "Unknown error")
            log.error(f"Task {task_id}: failed - {t.error}")

    else:
        log.warning(f"Unknown message type: {msg_type}")


# ---------------------------------------------------------------------------
# WebSocket: Data channels (parallel chunk transfer)
# ---------------------------------------------------------------------------
@app.websocket("/ws/data/{channel_id}")
async def ws_data(ws: WebSocket, channel_id: str):
    await ws.accept()
    data_channels[channel_id] = ws
    log.info(f"Data channel connected: {channel_id} (total: {len(data_channels)})")

    try:
        while True:
            data = await ws.receive_bytes()
            if len(data) < HEADER_SIZE:
                log.warning(f"Short frame on channel {channel_id}: {len(data)} bytes")
                continue

            # Parse header (48 bytes)
            task_uuid = data[:16]
            task_id_hex = task_uuid.hex()[:8]  # Use first 8 hex chars as task_id
            offset = struct.unpack(">Q", data[16:24])[0]
            chunk_size = struct.unpack(">I", data[24:28])[0]
            total_size = struct.unpack(">Q", data[28:36])[0]
            chunk_idx = struct.unpack(">I", data[36:40])[0]
            total_chunks = struct.unpack(">I", data[40:44])[0]
            # flags = struct.unpack(">I", data[44:48])[0]  # reserved

            payload = data[HEADER_SIZE:]

            # Find the task - use UUID5 mapping
            t = None
            mapped_tid = task_uuid_map.get(task_uuid)
            if mapped_tid:
                t = tasks.get(mapped_tid)
            if not t:
                # Fallback: try hex prefix match
                for tid, task in tasks.items():
                    if tid == task_id_hex or getattr(task, '_uuid_hex', '') == task_uuid.hex():
                        t = task
                        t._uuid_hex = task_uuid.hex()
                        break

            if not t:
                log.warning(f"Chunk for unknown task {task_id_hex}")
                continue

            # Update total_size / total_chunks from chunk header if pipeline mode
            if total_size > 0 and (t.total_size == 0 or getattr(t, '_pipeline_mode', False)):
                if t.total_size != total_size:
                    t.total_size = total_size
                    t.total_chunks = total_chunks
                    t._pipeline_mode = False
                    # Extend the file to the now-known size
                    if t._tmp_path:
                        try:
                            async with aiofiles.open(t._tmp_path, "r+b") as ef:
                                await ef.seek(total_size - 1)
                                await ef.write(b"\0")
                        except Exception:
                            pass

            # Sparse write
            if t._tmp_path:
                async with aiofiles.open(t._tmp_path, "r+b") as f:
                    await f.seek(offset)
                    await f.write(payload)

                t.received_chunks.add(chunk_idx)
                t.received_bytes += len(payload)
                if t.total_size > 0:
                    t.progress = int(t.received_bytes * 100 / t.total_size)

    except WebSocketDisconnect:
        log.info(f"Data channel disconnected: {channel_id}")
    except Exception as e:
        log.error(f"Data channel {channel_id} error: {e}")
    finally:
        data_channels.pop(channel_id, None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "gateway.server:app",
        host=BIND_HOST,
        port=BIND_PORT,
        log_level="info",
        ws_max_size=20 * 1024 * 1024,  # 20MB max WS frame
    )

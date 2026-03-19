#!/usr/bin/env python3
"""
IIISConnect bandwidth test — measures transfer speed with 1, 8, 16, 32 channels.

Run from iiis agent pod:
    python test_bandwidth.py --gateway wss://iiisconnect.iiis.co:7443

Or standalone to test WebSocket throughput between two endpoints.
"""
import asyncio
import json
import os
import struct
import time
import uuid

try:
    import websockets
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

GATEWAY_WS = os.getenv("IIISCONNECT_GATEWAY_WS", "wss://iiisconnect.iiis.co:7443/ws/agent")
GATEWAY_DATA_WS = os.getenv("IIISCONNECT_GATEWAY_DATA_WS", "wss://iiisconnect.iiis.co:7443/ws/data")
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB
HEADER_SIZE = 48
TEST_SIZE_MB = int(os.getenv("TEST_SIZE_MB", "256"))  # Total test data size in MB


def build_chunk(task_uuid: bytes, offset: int, chunk_size: int, total_size: int,
                chunk_idx: int, total_chunks: int) -> bytes:
    header = bytearray(HEADER_SIZE)
    header[0:16] = task_uuid
    struct.pack_into(">Q", header, 16, offset)
    struct.pack_into(">I", header, 24, chunk_size)
    struct.pack_into(">Q", header, 28, total_size)
    struct.pack_into(">I", header, 36, chunk_idx)
    struct.pack_into(">I", header, 40, total_chunks)
    struct.pack_into(">I", header, 44, 0)
    # Use random-ish data
    data = os.urandom(chunk_size)
    return bytes(header) + data


async def test_single_channel(n_channels: int, test_size_mb: int):
    """Test transfer with N parallel data channels."""
    total_bytes = test_size_mb * 1024 * 1024
    total_chunks = (total_bytes + CHUNK_SIZE - 1) // CHUNK_SIZE
    task_uuid = uuid.uuid4().bytes
    task_id = task_uuid.hex()[:8]

    print(f"\n{'='*60}")
    print(f"Testing with {n_channels} channel(s), {test_size_mb} MB data, {total_chunks} chunks")
    print(f"{'='*60}")

    # Connect control channel
    try:
        control_ws = await websockets.connect(GATEWAY_WS, max_size=20*1024*1024, ping_interval=30)
    except Exception as e:
        print(f"Failed to connect control channel: {e}")
        return None

    # Register
    await control_ws.send(json.dumps({
        "type": "register",
        "agent_id": f"bandwidth-test-{n_channels}ch",
        "capabilities": ["test"],
    }))

    # Notify transfer start (so gateway creates the temp file)
    await control_ws.send(json.dumps({
        "type": "transfer_start",
        "task_id": task_id,
        "filename": f"bandwidth_test_{n_channels}ch.bin",
        "size": total_bytes,
        "total_chunks": total_chunks,
    }))

    # Wait a moment for gateway to set up
    await asyncio.sleep(0.5)

    # Connect data channels
    data_ws_list = []
    for i in range(n_channels):
        ch_id = f"bw-test-{n_channels}ch-{i}"
        try:
            ws = await websockets.connect(
                f"{GATEWAY_DATA_WS}/{ch_id}",
                max_size=20 * 1024 * 1024,
                ping_interval=30,
            )
            data_ws_list.append(ws)
        except Exception as e:
            print(f"  Data channel {i} failed: {e}")

    active = len(data_ws_list)
    if active == 0:
        print("No data channels connected!")
        await control_ws.close()
        return None

    print(f"  Connected {active}/{n_channels} data channels")

    # Create chunk queue
    chunk_queue = asyncio.Queue()
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        size = min(CHUNK_SIZE, total_bytes - offset)
        chunk_queue.put_nowait((i, offset, size))

    sent_bytes = 0
    lock = asyncio.Lock()
    start_time = time.time()

    async def worker(ws, worker_id):
        nonlocal sent_bytes
        while True:
            try:
                chunk_idx, offset, size = chunk_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            frame = build_chunk(task_uuid, offset, size, total_bytes, chunk_idx, total_chunks)
            try:
                await ws.send(frame)
                async with lock:
                    sent_bytes += size
            except Exception as e:
                print(f"  Worker {worker_id} error: {e}")
                break

    # Run workers
    workers = [asyncio.create_task(worker(ws, i)) for i, ws in enumerate(data_ws_list)]

    # Progress reporter
    async def reporter():
        while any(not w.done() for w in workers):
            await asyncio.sleep(1)
            elapsed = time.time() - start_time
            speed = sent_bytes / elapsed if elapsed > 0 else 0
            pct = sent_bytes * 100 // total_bytes
            print(f"\r  Progress: {pct}% ({sent_bytes / 1e6:.1f}/{total_bytes / 1e6:.1f} MB) "
                  f"Speed: {speed / 1e6:.1f} MB/s", end="", flush=True)

    report_task = asyncio.create_task(reporter())
    await asyncio.gather(*workers)
    report_task.cancel()

    elapsed = time.time() - start_time
    speed = total_bytes / elapsed if elapsed > 0 else 0

    print(f"\n  Result: {total_bytes / 1e6:.1f} MB in {elapsed:.1f}s → {speed / 1e6:.1f} MB/s")

    # Notify completion
    await control_ws.send(json.dumps({
        "type": "transfer_complete",
        "task_id": task_id,
    }))

    # Cleanup
    for ws in data_ws_list:
        await ws.close()
    await control_ws.close()

    return speed


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="IIISConnect Bandwidth Test")
    parser.add_argument("--gateway", default=GATEWAY_WS, help="Gateway WebSocket URL")
    parser.add_argument("--data-ws", default=GATEWAY_DATA_WS, help="Data channel WS base URL")
    parser.add_argument("--size", type=int, default=TEST_SIZE_MB, help="Test data size in MB")
    parser.add_argument("--channels", nargs="+", type=int, default=[1, 4, 8, 16], help="Channel counts to test")
    args = parser.parse_args()

    global GATEWAY_WS, GATEWAY_DATA_WS, TEST_SIZE_MB
    GATEWAY_WS = args.gateway
    GATEWAY_DATA_WS = args.data_ws
    TEST_SIZE_MB = args.size

    print(f"IIISConnect Bandwidth Test")
    print(f"Gateway: {GATEWAY_WS}")
    print(f"Data WS: {GATEWAY_DATA_WS}")
    print(f"Test size: {TEST_SIZE_MB} MB")

    results = {}
    for n in args.channels:
        speed = await test_single_channel(n, TEST_SIZE_MB)
        if speed is not None:
            results[n] = speed
        await asyncio.sleep(2)

    # Summary
    print(f"\n{'='*60}")
    print(f"BANDWIDTH TEST RESULTS")
    print(f"{'='*60}")
    print(f"{'Channels':>10} {'Speed (MB/s)':>15} {'Speedup':>10}")
    print(f"{'-'*35}")
    baseline = results.get(1, results.get(min(results.keys())) if results else 1)
    for n, speed in sorted(results.items()):
        speedup = speed / baseline if baseline > 0 else 0
        print(f"{n:>10} {speed / 1e6:>14.1f} {speedup:>9.1f}x")
    print()


if __name__ == "__main__":
    asyncio.run(main())

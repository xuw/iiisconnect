#!/usr/bin/env python3
"""
IIISConnect CLI — submit downloads and check progress.

Usage:
    python iiisconnect.py download <url> [--dest /path/to/dir]
    python iiisconnect.py status <task_id>
    python iiisconnect.py jobs
    python iiisconnect.py cache
    python iiisconnect.py health
    python iiisconnect.py fetch <url> -o <output_file>
"""
import argparse
import json
import os
import sys
import time

try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q"])
    import requests

GATEWAY_URL = os.getenv("IIISCONNECT_URL", "https://iiisconnect.iiis.co:7443")


def fmt_size(b: float) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"


def fmt_speed(bps: float) -> str:
    return fmt_size(bps) + "/s"


def cmd_download(args):
    body = {"url": args.url}
    if args.dest:
        body["dest"] = args.dest

    r = requests.post(f"{GATEWAY_URL}/download", json=body, verify=False, timeout=30)
    r.raise_for_status()
    data = r.json()
    task_id = data["task_id"]
    print(f"Task submitted: {task_id}")
    print(f"Status: {data['status']}")

    if data["status"] == "completed":
        print(f"Cache hit! Path: {data.get('path', 'N/A')}")
        return

    if args.wait:
        print("Waiting for completion...")
        while True:
            time.sleep(2)
            s = requests.get(f"{GATEWAY_URL}/status/{task_id}", verify=False, timeout=10).json()
            status = s["status"]
            progress = s.get("progress", 0)
            speed = s.get("speed", 0)
            source = s.get("source", "")
            bar = "█" * (progress // 5) + "░" * (20 - progress // 5)
            print(f"\r  [{bar}] {progress}% {fmt_speed(speed)} via {source}  ", end="", flush=True)
            if status == "completed":
                duration = s.get("duration", 0)
                print(f"\n✅ Completed in {duration:.1f}s")
                return
            elif status == "failed":
                print(f"\n❌ Failed: {s.get('error', 'Unknown')}")
                sys.exit(1)


def cmd_status(args):
    r = requests.get(f"{GATEWAY_URL}/status/{args.task_id}", verify=False, timeout=10)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))


def cmd_jobs(args):
    r = requests.get(f"{GATEWAY_URL}/jobs", verify=False, timeout=10)
    r.raise_for_status()
    jobs = r.json()
    if not jobs:
        print("No jobs.")
        return
    print(f"{'ID':>10} {'Status':>12} {'Progress':>8} {'Speed':>12} {'Source':>20} {'URL'}")
    print("-" * 100)
    for j in jobs:
        print(f"{j['task_id']:>10} {j['status']:>12} {j.get('progress', 0):>6}% "
              f"{fmt_speed(j.get('speed', 0)):>12} {j.get('source', ''):>20} {j['url'][:50]}")


def cmd_cache(args):
    r = requests.get(f"{GATEWAY_URL}/cache", verify=False, timeout=10)
    r.raise_for_status()
    data = r.json()
    print(f"Cache: {data['total_entries']} entries, {data['total_size_gb']} GB")
    if data.get("entries"):
        print(f"\n{'Hash':>20} {'Size':>10} {'Filename'}")
        print("-" * 80)
        for e in data["entries"][:20]:
            print(f"{e['hash']:>20} {e['size_mb']:>8.1f} MB  {e['filename']}")


def cmd_health(args):
    r = requests.get(f"{GATEWAY_URL}/health", verify=False, timeout=10)
    r.raise_for_status()
    data = r.json()
    print(f"Status: {data['status']}")
    print(f"Agent connected: {data['agent_connected']}")
    print(f"Data channels: {data['data_channels']}")
    print(f"Cache: {data['cache_entries']} entries, {data['cache_size_gb']} GB")
    print(f"Active tasks: {data['active_tasks']}")


def cmd_fetch(args):
    url = args.url
    output = args.output or url.split("/")[-1].split("?")[0] or "download"

    print(f"Fetching: {url}")
    print(f"Output: {output}")

    r = requests.get(f"{GATEWAY_URL}/fetch", params={"url": url}, verify=False, stream=True, timeout=600)
    r.raise_for_status()

    total = int(r.headers.get("content-length", 0))
    downloaded = 0
    start = time.time()

    with open(output, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            elapsed = time.time() - start
            speed = downloaded / elapsed if elapsed > 0 else 0
            if total > 0:
                pct = downloaded * 100 // total
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                print(f"\r  [{bar}] {pct}% {fmt_size(downloaded)}/{fmt_size(total)} {fmt_speed(speed)}  ", end="", flush=True)
            else:
                print(f"\r  {fmt_size(downloaded)} {fmt_speed(speed)}  ", end="", flush=True)

    elapsed = time.time() - start
    print(f"\n✅ Downloaded {fmt_size(downloaded)} in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="IIISConnect CLI")
    parser.add_argument("--url", default=GATEWAY_URL, help="Gateway URL")
    sub = parser.add_subparsers(dest="command")

    dl = sub.add_parser("download", aliases=["dl"], help="Submit download")
    dl.add_argument("url", help="URL to download")  # positional, shadows --url
    dl.add_argument("--dest", "-d", help="Destination directory on xsx")
    dl.add_argument("--wait", "-w", action="store_true", default=True, help="Wait for completion")
    dl.add_argument("--no-wait", action="store_false", dest="wait")

    st = sub.add_parser("status", aliases=["st"], help="Check task status")
    st.add_argument("task_id")

    sub.add_parser("jobs", help="List jobs")
    sub.add_parser("cache", help="List cache")
    sub.add_parser("health", help="Health check")

    ft = sub.add_parser("fetch", help="Fetch file via gateway")
    ft.add_argument("url")  # positional
    ft.add_argument("-o", "--output", help="Output filename")

    args = parser.parse_args()

    # Use --url as gateway URL if no positional url shadows it
    global GATEWAY_URL
    if hasattr(args, 'url') and args.command not in ('download', 'dl', 'fetch'):
        GATEWAY_URL = args.url

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cmd_map = {
        "download": cmd_download, "dl": cmd_download,
        "status": cmd_status, "st": cmd_status,
        "jobs": cmd_jobs,
        "cache": cmd_cache,
        "health": cmd_health,
        "fetch": cmd_fetch,
    }
    cmd_map[args.command](args)


if __name__ == "__main__":
    main()

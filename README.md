# IIISConnect — Cross-Cluster Intelligent File Transfer

IIISConnect bridges the iiis cluster (Tsinghua IIIS) and xshixun (xsx) cluster, leveraging iiis's fast network access to download files and transfer them to xsx via parallel WebSocket streams.

## Problem

xsx cluster has limited internet access (proxy-only, ~5-6 MB/s). iiis cluster has fast access to Chinese mirrors (TUNA 93 MB/s, hf-mirror 9.2 MB/s). IIISConnect uses iiis as a download proxy for xsx.

## Architecture

```
  xsx Pod ──► iiisconnect-gateway (xsx) ◄──WebSocket──► iiisconnect-agent (iiis)
                  │                                           │
                  ├── Local cache (GPFS)                      ├── Smart routing
                  ├── REST API                                ├── China mirrors
                  └── Multi-channel data receive              └── Local cache (GFS)
```

- **Gateway** (xsx): FastAPI server with REST API + WebSocket endpoints
- **Agent** (iiis): Connects to gateway, downloads files, transfers via parallel WebSocket channels
- **Client**: CLI tool for submitting downloads and checking progress

## Key Features

- **Smart mirror routing**: Auto-replaces URLs with Chinese mirrors (hf-mirror, TUNA, ghproxy)
- **Parallel transfer**: Multiple WebSocket data channels (default 8) for high throughput
- **Dual-side caching**: Both agent and gateway maintain LRU caches (5TB each)
- **Chunked binary transfer**: 16MB chunks with sparse write support
- **Auto-reconnect**: Agent reconnects on disconnection with heartbeat monitoring

## Quick Start

### From xsx Pod

```bash
# Submit a download
curl -X POST https://iiisconnect.iiis.co:7443/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://huggingface.co/meta-llama/Llama-4-70B/resolve/main/model.safetensors"}'

# Check status
curl https://iiisconnect.iiis.co:7443/status/<task_id>

# Stream fetch
curl -o model.bin "https://iiisconnect.iiis.co:7443/fetch?url=https://..."

# Health check
curl https://iiisconnect.iiis.co:7443/health
```

### CLI Client

```bash
# Set gateway URL
export IIISCONNECT_URL=https://iiisconnect.iiis.co:7443

# Download with progress
python client/iiisconnect.py download https://huggingface.co/model/file.safetensors --dest /data/models/

# Check jobs
python client/iiisconnect.py jobs

# View cache
python client/iiisconnect.py cache
```

## Mirror Mapping

| Source | Mirror | Speed |
|--------|--------|-------|
| huggingface.co | hf-mirror.com | ~9.2 MB/s |
| github.com (releases) | ghproxy | ~2 MB/s |
| pypi.org | pypi.tuna.tsinghua.edu.cn | ~2.3 MB/s |
| npmjs.org | npmmirror.com | ~0.7 MB/s |
| ubuntu apt | mirrors.tuna.tsinghua.edu.cn | ~93 MB/s |

## Deployment

Gateway runs on xsx cluster, Agent on iiis cluster. See `k8s/` for manifests.

```bash
# Deploy gateway (xsx)
kubectl --context weixu-k8s.xa.cluster apply -f k8s/gateway-deploy.yaml -f k8s/gateway-svc.yaml -f k8s/gateway-ingress.yaml

# Deploy agent (iiis)
kubectl --context weixu-k8s.iiis apply -f k8s/agent-deploy.yaml
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check + agent status |
| `/download` | POST | Submit download request |
| `/status/{id}` | GET | Task status + progress |
| `/jobs` | GET | List all tasks |
| `/fetch?url=` | GET | Streaming file proxy |
| `/cache` | GET | List cached files |
| `/cache/{hash}` | DELETE | Remove cache entry |
| `/ws/agent` | WS | Agent control channel |
| `/ws/data/{id}` | WS | Parallel data channels |

## Transfer Protocol

- 1 control WebSocket (JSON messages) + N data WebSockets (binary chunks)
- Binary frame: 48-byte header + payload (up to 16MB)
- Header: task_uuid(16) + offset(8) + chunk_size(4) + total_size(8) + chunk_idx(4) + total_chunks(4) + flags(4)
- Gateway performs sparse writes by offset, supports out-of-order arrival

## License

MIT

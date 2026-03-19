# IIISConnect — 跨集群智能文件传输服务

## 1. 问题

xshixun (xsx) 集群下载外部资源困难：
- Pod 无法直连外网，必须走 HTTP 代理 (192.168.3.226:7890)
- 代理限速 5-6 MB/s，大文件不稳定（HuggingFace 超时）
- GitHub releases 经常失败
- huggingface.co 被墙

而 iiis 集群：
- 清华 TUNA 镜像 93 MB/s
- hf-mirror.com 9.2 MB/s
- 日本 AWS 中转可达 30-35 MB/s（国外源 → AWS 94 MB/s → iiis 35 MB/s）

**核心思路**：让 iiis 作为 xsx 的"下载代理"，利用 iiis 更快的网络帮 xsx 下载文件，再传过来。

---

## 2. 架构总览

```
                        ┌─────────────────────────────────────┐
                        │          xshixun 集群                │
                        │                                     │
  xsx Pod/用户 ────────►│  iiisconnect-gateway (Ingress)      │
  (下载请求)            │    ├── HTTPS 入口                    │
                        │    ├── 本地缓存 (PVC)               │
                        │    └── WebSocket 长连接 ◄────────┐  │
                        └──────────────────────────────────│──┘
                                                           │
                              WebSocket (wss://)           │
                              穿透防火墙                    │
                                                           │
                        ┌──────────────────────────────────│──┐
                        │          iiis 集群               │  │
                        │                                  │  │
                        │  iiisconnect-agent ──────────────┘  │
                        │    ├── 主动连接 xsx gateway         │
                        │    ├── 智能路由（选最快下载方式）    │
                        │    ├── 本地缓存 (PVC)               │
                        │    └── 下载后推送到 xsx             │
                        └─────────────────────────────────────┘
                                    │
                          ┌─────────┼─────────┐
                          ▼         ▼         ▼
                      国内镜像   本地缓存   AWS 日本中转
                     (TUNA/hf)             (SSH tunnel)
```

### 为什么用 WebSocket 而不是 iiis 开 Ingress？

- xsx 防火墙**阻止**对 iiis 外网 IP (122.200.68.28) 的访问
- 但 iiis 可以访问 xsx（61ms RTT）
- 所以 **iiis 主动连出**到 xsx 的 Ingress，建立 WebSocket 长连接
- 所有通信都走这条长连接，穿透防火墙

---

## 3. 组件详细设计

### 3.1 iiisconnect-gateway（xsx 集群）

**角色**：入口网关 + 本地缓存 + 任务管理

**K8s 资源**：
- Deployment: 1 replica, 0 GPU, 0.5 CPU / 1Gi RAM
- Service: ClusterIP, port 8000
- Ingress: `connect.xshixun.iiis.co` (或 `models-xshixun.iiis.co/connect`)
- PVC: 缓存存储（复用 `pvc-gpfshome-weixu` 下的 `/data/iiisconnect-cache/`）

**API 端点**：

```
# 文件下载（对用户/Pod 暴露）
POST /download
  body: { "url": "https://huggingface.co/...", "dest": "/data/models/xxx" }
  response: { "task_id": "abc123", "status": "queued" }

GET /status/{task_id}
  response: { "status": "downloading", "progress": 45, "speed": "32 MB/s", "source": "aws-relay" }

# 直接获取文件（像 HTTP 代理一样用）
GET /fetch?url=https://huggingface.co/model.safetensors
  → 流式返回文件内容（优先本地缓存，否则走 iiis 下载）

# 缓存管理
GET /cache
  → 列出缓存文件
DELETE /cache/{hash}
  → 清理缓存

# 健康检查
GET /health
  → { "status": "ok", "iiis_connected": true, "cache_size": "12.3 GB" }

# WebSocket（供 iiis agent 连接）
WS /ws/agent
  → iiis agent 的控制通道
```

**内部逻辑**：
1. 收到下载请求 → 检查本地缓存
2. 缓存命中 → 直接返回/复制到目标路径
3. 缓存未命中 → 通过 WebSocket 发任务给 iiis agent
4. iiis agent 下载完成 → 通过连接推送文件（chunked）到 gateway
5. gateway 存入缓存 + 写入目标路径

### 3.2 iiisconnect-agent（iiis 集群）

**角色**：智能下载器 + 路由选择 + 缓存

**K8s 资源**：
- Deployment: 1 replica, 0 GPU, 1 CPU / 2Gi RAM
- PVC: 缓存存储（`gfs-nvme-pvc-share-weixu` 下的 `/data/iiisconnect-cache/`）
- 可选：SSH key Secret（用于 AWS 日本中转）

**无 Ingress**：主动连接 xsx gateway 的 WebSocket

**智能路由引擎**：

收到下载 URL 后，按以下策略选择下载方式：

```python
ROUTES = [
    {
        "name": "local-cache",
        "desc": "本地缓存命中",
        "check": lambda url: cache_exists(url),
        "speed": "instant",
        "priority": 0,
    },
    {
        "name": "china-mirror",
        "desc": "国内镜像替换",
        "check": lambda url: has_china_mirror(url),
        "speed": "9-93 MB/s",
        "priority": 1,
        "mirrors": {
            "huggingface.co": "hf-mirror.com",           # 9.2 MB/s
            "github.com/*/releases": "mirror.ghproxy.com", # 或 kkgithub.com
            "pypi.org": "pypi.tuna.tsinghua.edu.cn",      # 2.3 MB/s
            "registry.npmjs.org": "registry.npmmirror.com", # 0.7 MB/s
            "archive.ubuntu.com": "mirrors.tuna.tsinghua.edu.cn", # 93 MB/s
        },
    },
    {
        "name": "aws-relay",
        "desc": "日本 AWS 中转",
        "check": lambda url: is_foreign_url(url),
        "speed": "30-35 MB/s (relay) or limited by source",
        "priority": 2,
        "method": "ssh-tunnel",  # SSH 到 AWS，curl 下载，rsync/HTTP 回传
    },
    {
        "name": "direct",
        "desc": "iiis 直连",
        "check": lambda url: True,  # fallback
        "speed": "4-10 MB/s",
        "priority": 3,
    },
]
```

**镜像映射规则（详细）**：

| 原始域名 | 镜像 | 预期速度 | 备注 |
|----------|------|---------|------|
| huggingface.co | hf-mirror.com | 9.2 MB/s | 模型下载主力 |
| github.com (releases) | 直连或 ghproxy | 0.08-2 MB/s | GitHub 限速严重 |
| pypi.org | pypi.tuna.tsinghua.edu.cn | 2.3 MB/s | pip 包 |
| registry.npmjs.org | registry.npmmirror.com | 0.7 MB/s | npm 包 |
| *.ubuntu.com | mirrors.tuna.tsinghua.edu.cn | 93 MB/s | apt 包 |
| docker.io / gcr.io | 各种国内镜像 | 变化大 | 容器镜像 |

**AWS 中转流程**：

```
1. agent SSH 到日本 AWS (13.208.212.186)
2. 在 AWS 上 curl 下载文件到 /tmp/（94 MB/s）
3. 从 iiis rsync/HTTP 拉回（30-35 MB/s）
4. 删除 AWS 上的临时文件
```

适用于：HuggingFace 官方（被墙）、GitHub releases（国内限速）、其他国外源。

### 3.3 WebSocket 协议

Gateway ↔ Agent 之间的通信协议：

```json
// Agent → Gateway: 注册
{"type": "register", "agent_id": "iiis-01", "capabilities": ["mirror", "aws-relay", "direct"]}

// Gateway → Agent: 下载任务
{"type": "task", "task_id": "abc123", "url": "https://huggingface.co/...", "sha256": "optional"}

// Agent → Gateway: 进度更新
{"type": "progress", "task_id": "abc123", "status": "downloading", "progress": 45, "speed": 32000000, "source": "china-mirror", "eta_seconds": 120}

// Agent → Gateway: 文件传输开始
{"type": "transfer_start", "task_id": "abc123", "filename": "model.safetensors", "size": 4500000000, "sha256": "..."}

// Agent → Gateway: 文件数据（binary frames）
[binary data chunks, 1MB each]

// Agent → Gateway: 传输完成
{"type": "transfer_complete", "task_id": "abc123", "sha256_verified": true}

// Agent → Gateway: 错误
{"type": "error", "task_id": "abc123", "error": "Mirror timeout, retrying with aws-relay"}
```

**大文件传输**：WebSocket binary frames，每 chunk 1MB，带进度。

**断线重连**：Agent 每 10 秒心跳，断线后自动重连，未完成的任务自动恢复。

---

## 4. 缓存策略

两端各维护独立缓存：

**缓存 key**：URL 的 SHA256（去掉 query string 中的 token 等临时参数）

**缓存目录结构**：
```
/data/iiisconnect-cache/
├── files/
│   ├── a1b2c3d4.../  (URL hash 前 8 位)
│   │   └── model.safetensors
│   └── ...
├── metadata.json      (缓存索引: URL → hash → size → timestamp)
└── stats.json         (下载统计)
```

**淘汰策略**：
- 默认缓存上限 100GB（可配置）
- LRU 淘汰：最久未访问的先删
- 手动清理：`DELETE /cache/{hash}`

**缓存一致性**：
- 如果 URL 带 ETag/Last-Modified → 条件请求验证
- HuggingFace 模型用 commit hash 做版本（URL 里有 revision）
- 不带版本信息的 URL → TTL 24 小时后重新验证

---

## 5. 传输速度预估

| 场景 | 方式 | 瓶颈 | 端到端速度 |
|------|------|------|-----------|
| HuggingFace 模型 → xsx | 国内镜像 → iiis → WebSocket → xsx | iiis 下载 9.2 MB/s | **~9 MB/s** |
| HuggingFace 被墙模型 → xsx | AWS 下载 → iiis → WebSocket → xsx | AWS→iiis 35 MB/s, 但 iiis→xsx WebSocket 带宽待测 | **~10-20 MB/s** (估) |
| GitHub release → xsx | 镜像/AWS 中转 → iiis → xsx | GitHub 源本身慢 | **~2-5 MB/s** |
| pip/npm 包 → xsx | 国内镜像 → iiis → xsx | 小文件 SSL 开销 | **~2 MB/s** |
| APT 包 → xsx | TUNA → iiis → xsx | WebSocket 带宽 | **~20-30 MB/s** (估) |
| 缓存命中 → xsx | 直接 WebSocket 传输 | WebSocket 带宽 | **~15-25 MB/s** (估) |

> ⚠️ iiis→xsx 通过 WebSocket 的实际传输带宽需要测试。理论上 iiis→xsx RTT 61ms，WebSocket over TLS 应该能到 15-25 MB/s。

---

## 6. K8s 部署清单

### 6.1 xsx 集群

```
ltx-scaler               ← 已有（LTX 视频服务）
iiisconnect-gateway       ← 新建
├── Deployment (1 replica, 0 GPU, 0.5 CPU / 1Gi)
├── Service (ClusterIP, port 8000)
├── Ingress (connect.xshixun.iiis.co 或 models-xshixun.iiis.co/connect)
└── PVC (复用 pvc-gpfshome-weixu → /data/iiisconnect-cache/)
```

### 6.2 iiis 集群

```
iiisconnect-agent         ← 新建
├── Deployment (1 replica, 0 GPU, 1 CPU / 2Gi)
├── PVC (gfs-nvme-pvc-share-weixu → /data/iiisconnect-cache/)
├── Secret (aws-ssh-key, 用于 AWS 中转)
└── 无 Ingress（主动外连）
```

---

## 7. 技术栈

- **语言**: Python 3 (FastAPI + websockets)
- **依赖**: fastapi, uvicorn, websockets, httpx (异步 HTTP client), aiofiles
- **无外部存储依赖**: 纯内存队列 + 文件系统缓存
- **容器镜像**: 复用 `lab-cpu:latest`，运行时 pip install

---

## 8. 客户端使用示例

### 8.1 xsx Pod 内直接使用

```bash
# 下载 HuggingFace 模型到指定路径
curl -X POST https://models-xshixun.iiis.co:7443/connect/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://huggingface.co/Lightricks/LTX-2.3/resolve/main/ltx-2.3-22b-distilled.safetensors", "dest": "/data/models/LTX-2.3/"}'

# 查询进度
curl https://models-xshixun.iiis.co:7443/connect/status/abc123

# 像 HTTP 代理一样获取文件（流式）
curl -o model.bin "https://models-xshixun.iiis.co:7443/connect/fetch?url=https://huggingface.co/..."

# 当 pip/apt 代理（环境变量方式，后续扩展）
# export PIP_INDEX_URL=https://models-xshixun.iiis.co:7443/connect/pypi/simple
```

### 8.2 Python 客户端

```python
import requests

# 提交下载
r = requests.post("https://models-xshixun.iiis.co:7443/connect/download", json={
    "url": "https://huggingface.co/meta-llama/Llama-4-70B/...",
    "dest": "/data/models/llama4/"
})
task_id = r.json()["task_id"]

# 轮询等待
while True:
    s = requests.get(f"https://models-xshixun.iiis.co:7443/connect/status/{task_id}").json()
    print(f"{s['progress']}% - {s['speed']} - via {s['source']}")
    if s["status"] in ("completed", "failed"):
        break
    time.sleep(5)
```

---

## 9. 扩展能力（后续）

1. **pip/apt/npm 代理模式** — 直接替代 xsx 集群的包管理器源
2. **Docker 镜像代理** — crane pull through
3. **多 agent 支持** — 多个 iiis agent 同时连接，负载均衡
4. **Web UI** — 可视化下载进度、缓存管理、路由统计
5. **API key 认证** — 多用户隔离
6. **断点续传** — 大文件中断后从断点继续

---

## 10. 存储规划

### iiis 集群缓存
- **PVC**: `gfs-sata-share-pvc-weixu` (9.3TB, RWX, kadalu.gfs-sata-share-ib)
- **挂载路径**: `/cache` (agent pod)
- **缓存目录**: `/cache/iiisconnect/`
- **缓存上限**: 5TB（留余量给其他用途）
- **用途**: 缓存所有下载过的模型、安装包、数据文件
- **优势**: 基于 SATA 磁盘的 GlusterFS，大容量持久存储

### xsx 集群缓存
- **PVC**: `pvc-gpfshome-weixu` (488TB, RWX, GPFS)
- **挂载路径**: `/data` (gateway pod)
- **缓存目录**: `/data/iiisconnect-cache/`
- **缓存上限**: 5TB
- **用途**: 缓存已传输到 xsx 的文件，避免重复传输

---

## 11. 多线程高并发分块传输

### 设计目标
单 WebSocket 连接带宽可能受限（TCP 单流 + TLS 开销），通过多连接并行传输突破瓶颈。

### 方案：多 WebSocket 并行分块传输

```
Agent (iiis)                          Gateway (xsx)
    │                                      │
    ├── WS control channel ────────────────┤  (控制通道，任务调度)
    │                                      │
    ├── WS data channel #1 ───[chunk 0]───►│
    ├── WS data channel #2 ───[chunk 1]───►│  (N 条并行数据通道)
    ├── WS data channel #3 ───[chunk 2]───►│
    ├── WS data channel #4 ───[chunk 3]───►│
    │         ...                          │
    └── WS data channel #N ───[chunk M]───►│
                                           │
                                    [重组 chunks → 完整文件]
```

### 实现细节

**Agent 端**：
- 1 条 WebSocket 控制通道（心跳、任务调度、状态）
- N 条 WebSocket 数据通道（默认 8，可配置 1-32）
- 大文件分块：每块 16MB
- 每条数据通道独立发送不同的 chunk
- 支持乱序到达，Gateway 按 offset 重组

**Gateway 端**：
- 接受 N+1 条 WebSocket 连接（1 control + N data）
- 收到 chunk 后按 (task_id, offset) 写入临时文件
- 所有 chunks 到齐后合并（或直接 sparse write 到最终文件）
- 进度跟踪：已接收 chunks / 总 chunks

**数据通道协议**：
```
# 每个 binary frame 的 header (固定 48 bytes)
struct ChunkHeader {
    task_id:    [16 bytes, UUID]
    offset:     [8 bytes, uint64, 文件内偏移]
    chunk_size: [4 bytes, uint32, 本 chunk 大小]
    total_size: [8 bytes, uint64, 文件总大小]
    chunk_idx:  [4 bytes, uint32, chunk 序号]
    total_chunks: [4 bytes, uint32, 总 chunk 数]
    flags:      [4 bytes, 保留]
}
# 后接 chunk_size bytes 的实际数据
```

**自适应并发**：
- 启动时开 4 条数据通道测试
- 监控每条通道的实际吞吐量
- 如果总带宽未饱和（< 上次测量的 90%），增加通道数（最多 32）
- 如果某条通道明显慢于其他，关闭它减少争抢

### 预期效果

| 场景 | 单连接 | 8 并发 | 16 并发 |
|------|--------|--------|---------|
| iiis → xsx (理论) | ~15-20 MB/s | ~40-80 MB/s | ~60-100 MB/s |
| 实际需测试 | - | - | - |

> 多连接能否线性提升取决于：中间网络路径、两端磁盘 I/O、TLS 开销。SATA GFS 的读写速度也是瓶颈之一。

---

## 12. 开发计划

| 阶段 | 内容 | 预估时间 |
|------|------|---------|
| P0 | Gateway 基本框架 + WebSocket (control + data channels) | 1.5h |
| P0 | Agent 智能路由 + 国内镜像 + 直连下载 | 1h |
| P0 | 多线程分块传输（Agent → Gateway） | 1.5h |
| P0 | 缓存系统 (SATA GFS) + 状态查询 | 30min |
| P0 | K8s 部署 + RBAC + Ingress + 端到端测试 | 1h |
| P0 | 带宽测试（单连接 vs 多连接对比） | 30min |
| P0 | Git 初始化 + commit 到 GitHub | 15min |
| P1 | AWS 日本 SSH 中转 | 1h |
| P1 | 断线重连 + 任务恢复 | 30min |

**P0 总计约 6-7 小时**。

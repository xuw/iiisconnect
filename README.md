# IIISConnect

**跨集群模型/数据高速传输系统**

自动加速 HuggingFace、GitHub、PyPI 等国外资源的下载，从 iiis 集群（高速国际出口）传输到 xshixun 集群（隔离内网）。

## 架构

```
xshixun pod → Gateway (xsx) ←WebSocket→ Agent (iiis) → 国内镜像/AWS中转 → 下载
                 ↓                           ↓
              缓存 (5TB)                  缓存 (5TB)
```

- **Pipeline 模式**: 下载和传输并行，端到端约 6.6 MB/s
- **智能路由**: HuggingFace→hf-mirror.com, GitHub→ghproxy, PyPI→清华源
- **AWS 中转**: 纯海外链接自动走日本 AWS 节点 (30-35 MB/s 到 iiis)
- **双端缓存**: 重复下载秒返回

## 使用方式

### 方式 1: HTTP 代理（推荐）

在 xshixun 集群的 Pod 里设置环境变量：

```bash
export http_proxy=http://iiisconnect-gateway:3128
export https_proxy=http://iiisconnect-gateway:3128
export no_proxy=localhost,127.0.0.1,.svc,.cluster
```

然后所有工具自动走加速：

```bash
# pip 自动加速
pip install transformers torch

# curl/wget 自动加速
curl -O http://files.pythonhosted.org/.../package.whl

# huggingface_hub 自动加速
huggingface-cli download bert-base-uncased

# git clone (HTTP) 自动加速
git clone https://github.com/owner/repo.git
```

**注意**: 当前版本 HTTP 代理对 HTTPS 域名使用透明 CONNECT tunnel（不加速）。如需加速 HTTPS 下载，请使用方式 2 的 REST API。

### 方式 2: REST API

提交下载任务：

```bash
curl -X POST http://iiisconnect-gateway/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://huggingface.co/Qwen/Qwen2.5-7B/resolve/main/model.safetensors"}'

# 返回: {"task_id": "a1b2c3d4", "status": "queued"}
```

查看进度：

```bash
curl http://iiisconnect-gateway/status/a1b2c3d4
# {"status": "pipeline", "progress": 45, "speed": 6500000, "source": "china-mirror:huggingface"}
```

直接获取文件（阻塞到完成）：

```bash
curl -o model.bin "http://iiisconnect-gateway/fetch?url=https://huggingface.co/.../model.safetensors"
```

### 方式 3: Python (requests/httpx)

```python
import requests

# 使用代理
proxies = {"http": "http://iiisconnect-gateway:3128"}
resp = requests.get("http://...", proxies=proxies)

# 或使用 REST API
resp = requests.post("http://iiisconnect-gateway/download", json={"url": "https://..."})
task_id = resp.json()["task_id"]
# ... poll status ...
```

## 性能

| 场景 | 速度 | 说明 |
|------|------|------|
| HuggingFace 镜像 | ~9 MB/s | iiis agent 下载 |
| GitHub 镜像 | ~2 MB/s | iiis agent 下载 |
| 纯海外链接 (AWS 中转) | ~30 MB/s | 日本 AWS → iiis |
| iiis → xsx WebSocket | ~7 MB/s | 物理链路瓶颈 |
| **端到端 (Pipeline)** | **~6.6 MB/s** | 下载+传输并行 |

示例：Qwen2.5-0.5B (988MB) 约 152 秒完成。

## 支持的加速域名

- `huggingface.co` → hf-mirror.com
- `cdn-lfs.huggingface.co` → hf-mirror.com
- `github.com` (releases/archive) → ghproxy
- `files.pythonhosted.org` / `pypi.org` → 清华源
- `registry.npmjs.org` → npmmirror
- `archive.ubuntu.com` → 清华源

其他域名：
- 国外 IP 自动走 AWS 日本中转
- 国内可达的直连

## 其他命令

```bash
# 健康检查
curl http://iiisconnect-gateway/health

# 查看所有任务
curl http://iiisconnect-gateway/jobs

# 查看缓存
curl http://iiisconnect-gateway/cache

# 删除缓存
curl -X DELETE http://iiisconnect-gateway/cache/<hash_prefix>
```

## 部署

**xshixun 集群 (Gateway)**:
```bash
kubectl apply -f k8s/gateway-deploy.yaml
kubectl apply -f k8s/gateway-service.yaml
kubectl apply -f k8s/gateway-ingress.yaml
```

**iiis 集群 (Agent)**:
```bash
kubectl apply -f k8s/agent-deploy.yaml
```

Gateway 自动启动两个服务：
- 端口 8000: REST API + WebSocket
- 端口 3128: HTTP 代理

## 安全

- **Ingress 只暴露 `/ws/*` 和 `/health`**: API 端点 (`/download`, `/fetch`, `/jobs`, `/cache`) 只能集群内访问
- **代理端口 (3128) 不暴露外网**: 只有 ClusterIP Service
- Agent WebSocket 连接需通过外网 Ingress（跨集群必需）

## License

MIT

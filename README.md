# archive-proxy

中文 | [English](./README.en.md)

`archive-proxy` 是一个放在 LiteLLM 前面的透明归档代理。客户端继续使用 OpenAI / Anthropic 兼容接口，请求会转发到 LiteLLM，同时代理把请求、响应、响应头、流式 chunk、usage 和可重建文本摘要写入本地文件，方便审计、对账、月度导出和问题排查。

默认链路：

```text
client -> archive-proxy:8000 -> LiteLLM:4000 -> upstream OpenAI / Anthropic compatible service
```

Docker Compose 默认只把代理暴露在 `127.0.0.1:56789`，LiteLLM 仅在 Compose 网络内可访问。

## 功能

- 透明转发 LiteLLM 支持的 HTTP 请求。
- 自动识别 OpenAI / Anthropic 请求族。
- 支持普通 JSON 响应和 SSE 流式响应归档。
- 支持按 session 分目录，来源包括 `x-archive-session-id`、`x-session-id`、`conversation_id` 等。
- 写入 `index.<pid>.jsonl`，适合多 uvicorn worker 并发运行。
- 可选 header 脱敏，避免把 `Authorization`、`x-api-key`、Cookie 等敏感头写入归档。
- 使用有界后台归档队列，优先保护代理延迟和可用性。
- 附带 New API 定价抓取、月度导出和压测脚本。

## 快速开始

### 1. 准备配置

```bash
cp .env.example .env
cp .env.litellm.example .env.litellm
cp litellm-config.example.yaml litellm-config.yaml
```

编辑 `.env.litellm`：

```env
LITELLM_MASTER_KEY=sk-your-local-master-key
UPSTREAM_API_KEY=sk-your-upstream-api-key
UPSTREAM_OPENAI_BASE=https://your-new-api.example.com/v1
UPSTREAM_ANTHROPIC_BASE=https://your-new-api.example.com
```

`.env.example` 默认启用 header 脱敏：

```env
ARCHIVE_REDACT_HEADERS=true
```

### 2. 启动

```bash
docker compose up -d --build
```

检查服务：

```bash
docker compose ps
curl http://127.0.0.1:56789/_archive/health
curl http://127.0.0.1:56789/_archive/stats
```

### 3. 调用 OpenAI 兼容接口

```bash
curl http://127.0.0.1:56789/v1/chat/completions \
  -H "Authorization: Bearer sk-your-local-master-key" \
  -H "Content-Type: application/json" \
  -H "x-archive-session-id: demo-session-001" \
  -d '{
    "model": "openai/gpt-4o-mini",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

### 4. 调用 Anthropic 兼容接口

```bash
curl http://127.0.0.1:56789/v1/messages \
  -H "x-api-key: sk-your-local-master-key" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "x-archive-session-id: demo-session-002" \
  -d '{
    "model": "anthropic/claude-3-5-sonnet-latest",
    "max_tokens": 256,
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

## 配置

### archive-proxy

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `UPSTREAM_BASE_URL` | `http://litellm:4000` | LiteLLM 上游地址。Docker Compose 内保持默认即可。 |
| `ARCHIVE_ROOT` | `/data/archive` | 归档文件保存目录。 |
| `DEFAULT_FAMILY` | `openai` | 无法识别请求族时的默认族，可选 `openai` 或 `anthropic`。 |
| `ARCHIVE_MAX_INLINE_BODY_BYTES` | `52428800` | 单个非流式 body 最大内联保存大小，超过后只保存元数据。 |
| `ARCHIVE_ADD_REQUEST_ID_HEADER` | `false` | 是否向上游追加 `x-archive-request-id`。 |
| `ARCHIVE_FORWARD_TIMEOUT_SECONDS` | `0` | 转发总超时，`0` 表示不设置总超时。 |
| `ARCHIVE_REDACT_HEADERS` | `false` | 是否在归档 header 时脱敏敏感头；示例配置设为 `true`。 |
| `ARCHIVE_STREAM_FLUSH_CHUNKS` | `32` | 流式归档累计多少个 chunk 后 flush。 |
| `ARCHIVE_STREAM_FLUSH_BYTES` | `262144` | 流式归档累计多少字节后 flush。 |
| `ARCHIVE_SHUTDOWN_DRAIN_SECONDS` | `5` | 正常关闭时等待后台归档队列收尾的秒数。 |
| `ARCHIVE_QUEUE_MAXSIZE` | `20000` | 后台归档队列最大长度。队列满时会丢弃 best-effort 归档以保护请求延迟。 |
| `ARCHIVE_WRITER_WORKERS` | `4` | 每个 uvicorn worker 内的后台归档 writer 数量。 |
| `ARCHIVE_INDEX_MODE` | `worker` | `worker` 写 `index.<pid>.jsonl`，适合多 worker；`shared` 写旧版共享 `index.jsonl`。 |

### LiteLLM

| 变量 | 说明 |
| --- | --- |
| `LITELLM_MASTER_KEY` | 客户端访问本地 LiteLLM 的 master key。 |
| `UPSTREAM_API_KEY` | 上游 OpenAI / Anthropic 兼容服务的 API key。 |
| `UPSTREAM_OPENAI_BASE` | OpenAI 兼容接口 base URL，通常以 `/v1` 结尾。 |
| `UPSTREAM_ANTHROPIC_BASE` | Anthropic 兼容接口 base URL，通常不带 `/v1`。 |

`litellm-config.example.yaml` 默认配置：

- `openai/*` 转发到 `UPSTREAM_OPENAI_BASE`。
- `anthropic/*` 转发到 `UPSTREAM_ANTHROPIC_BASE`。
- `master_key` 从 `LITELLM_MASTER_KEY` 读取。

## 归档行为

每次请求会生成一个 request id，并根据请求族、session id 和时间戳写入归档目录。典型结构：

```text
archives/
  index.<pid>.jsonl
  openai/
    index.<pid>.jsonl
    no_session/
    session/
  anthropic/
    index.<pid>.jsonl
    no_session/
    session/
```

常见文件：

- `*-req.json`：请求元数据和请求体。
- `*-headers.json`：客户端请求头、转发给 LiteLLM 的请求头、上游响应头。
- `*-res.json`：非流式响应体，或流式响应的 meta、summary、usage、错误和重建文本摘要。
- `*-chunks.jsonl`：仅流式请求生成，一行一个响应 chunk。
- `index.<pid>.jsonl`：索引行，包含模型、状态码、usage、文件路径和 `archive_status`。

归档写入优先保护请求链路：非流式成功请求的归档在后台队列执行；上游异常等关键错误记录在队列满时会 inline 写入；流式 chunk 边转发边写 JSONL，并按 chunk 数或字节数批量 flush。极端积压时，best-effort 归档可能被丢弃，`/_archive/stats` 会记录 `dropped` 和 `dropped_best_effort`。

## 管理接口

```bash
curl http://127.0.0.1:56789/_archive/health
curl http://127.0.0.1:56789/_archive/stats
```

`/_archive/stats` 包含：

- `index_mode`、`queue_maxsize`、`writer_workers`：当前归档配置。
- `archive_queue.enqueued`、`completed`、`failed`：后台任务统计。
- `archive_queue.queued`、`writers`：当前积压和 writer 数量。
- `archive_queue.dropped`、`dropped_best_effort`：因为队列满或队列不可用丢弃的归档。
- `archive_queue.overflow_inline`：非 best-effort 任务在队列满时改为 inline 写入的次数。

## 月度导出

先准备本地工具配置：

```bash
cp archive-tools.example.yaml archive-tools.yaml
```

编辑 `archive-tools.yaml`，填入自己的 New API pricing 地址、归档目录和导出目录。该文件默认被 `.gitignore` 排除。

常规运行：

```bash
python3 tools/fetch_newapi_pricing.py
python3 tools/archive_monthly_export.py
```

导出指定月份：

```bash
python3 tools/fetch_newapi_pricing.py \
  --url https://your-new-api.example.com/pricing \
  --out tools/newapi_pricing.json

python3 tools/archive_monthly_export.py \
  --archive-root ./archives \
  --pricing ./tools/newapi_pricing.json \
  --month 2026-04 \
  --out-dir ./tools/monthly_exports/2026-04 \
  --mode all
```

导出结果包括 `summary.json`、`by_model.csv`、`by_model.json`、`missing_pricing.csv`、合并后的 JSONL 分片和 raw archive ZIP 分片。导出工具默认读取 `index*.jsonl`，也可以用 `--walk` 扫描 `*-res.json`。

`monthly_export.month` 支持：

- `previous`：上一个 UTC 月，适合每月固定导出。
- `current`：当前 UTC 月。
- `YYYY-MM`：固定月份，例如 `2026-04`。

## 压测

```bash
python3 tools/benchmark_proxy.py \
  --url http://127.0.0.1:56789/v1/chat/completions \
  --compare-url http://127.0.0.1:4000/v1/chat/completions \
  --api-key sk-your-local-master-key \
  --model openai/gpt-4o-mini \
  --requests 100 \
  --concurrency 10 \
  --max-tokens 64
```

流式压测：

```bash
python3 tools/benchmark_proxy.py \
  --url http://127.0.0.1:56789/v1/chat/completions \
  --compare-url http://127.0.0.1:4000/v1/chat/completions \
  --api-key sk-your-local-master-key \
  --model openai/gpt-4o-mini \
  --requests 50 \
  --concurrency 5 \
  --max-tokens 128 \
  --stream
```

重点字段：

- `ok_rps`：成功请求吞吐。
- `latency_ms.p95` / `latency_ms.p99`：尾延迟。
- `first_byte_ms.p95`：流式首字节延迟。
- `bytes_per_second` / `mbit_per_second`：响应读取速率。
- `comparison_target_over_compare`：代理相对直连的倍数。

压测前后建议查看 `/_archive/stats`，确认 `archive_queue.queued`、`failed`、`dropped` 是否异常。

## 本地开发

不使用 Docker 时可以直接运行代理：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
UPSTREAM_BASE_URL=http://127.0.0.1:4000 ARCHIVE_ROOT=./archives uvicorn archive_proxy:app --host 0.0.0.0 --port 8000
```

LiteLLM 可以参考官方方式启动，并使用 `litellm-config.yaml` 作为配置文件。

## 开源与安全

仓库默认不会提交真实密钥和运行数据。只提交 example 文件，不要提交：

- `.env`
- `.env.litellm`
- `litellm-config.yaml`
- `archive-tools.yaml`
- `archives/`
- `tools/monthly_exports/`
- `tools/newapi_pricing.json`

生产环境注意：

- 归档内容可能包含用户提示词、模型输出和业务数据，请限制 `archives/` 目录权限。
- `ARCHIVE_REDACT_HEADERS=true` 只脱敏 header，不脱敏请求体或响应体。
- 如果代理对公网开放，建议放在 Nginx、Caddy 或 API Gateway 后面，并配置 TLS、访问控制和限流。
- `archives/` 会持续增长，需要结合业务保留周期做日志轮转、对象存储归档或定期清理。
- 如果真实密钥曾经进入 Git 历史，公开仓库前需要清理历史并轮换密钥。

## 许可证

本项目基于 [MIT License](./LICENSE) 开源。

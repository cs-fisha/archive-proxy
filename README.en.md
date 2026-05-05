# archive-proxy

[中文](./README.md) | English

`archive-proxy` is a transparent archiving proxy in front of LiteLLM. Clients keep using OpenAI / Anthropic compatible APIs, requests are forwarded to LiteLLM, and the proxy stores request bodies, response bodies, headers, streaming chunks, usage data, and reconstructed text summaries on disk for auditing, billing reconciliation, monthly exports, and debugging.

Default flow:

```text
client -> archive-proxy:8000 -> LiteLLM:4000 -> upstream OpenAI / Anthropic compatible service
```

The included Docker Compose setup exposes only the proxy on `127.0.0.1:56789`. LiteLLM stays internal to the Compose network.

## Features

- Transparent forwarding for LiteLLM HTTP requests.
- Automatic OpenAI / Anthropic request-family detection.
- Archives both regular JSON responses and SSE streaming responses.
- Session-aware directory layout from `x-archive-session-id`, `x-session-id`, `conversation_id`, and related fields.
- Writes `index.<pid>.jsonl`, which is safe for multiple uvicorn workers.
- Optional header redaction for `Authorization`, `x-api-key`, cookies, and related sensitive headers.
- Bounded background archive queue that protects proxy latency and availability.
- Includes New API pricing fetch, monthly export, and benchmark helpers.

## Quick Start

### 1. Prepare config

```bash
cp .env.example .env
cp .env.litellm.example .env.litellm
cp litellm-config.example.yaml litellm-config.yaml
```

Edit `.env.litellm`:

```env
LITELLM_MASTER_KEY=sk-your-local-master-key
UPSTREAM_API_KEY=sk-your-upstream-api-key
UPSTREAM_OPENAI_BASE=https://your-new-api.example.com/v1
UPSTREAM_ANTHROPIC_BASE=https://your-new-api.example.com
```

`.env.example` enables header redaction by default:

```env
ARCHIVE_REDACT_HEADERS=true
```

### 2. Start

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
curl http://127.0.0.1:56789/_archive/health
curl http://127.0.0.1:56789/_archive/stats
```

### 3. Call an OpenAI-compatible endpoint

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

### 4. Call an Anthropic-compatible endpoint

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

## Configuration

### archive-proxy

| Variable | Default | Description |
| --- | --- | --- |
| `UPSTREAM_BASE_URL` | `http://litellm:4000` | Upstream LiteLLM base URL. Keep the default in Docker Compose. |
| `ARCHIVE_ROOT` | `/data/archive` | Directory for archive files. |
| `DEFAULT_FAMILY` | `openai` | Fallback family when auto-detection is inconclusive. Use `openai` or `anthropic`. |
| `ARCHIVE_MAX_INLINE_BODY_BYTES` | `52428800` | Maximum non-streaming body size stored inline. Larger bodies store metadata only. |
| `ARCHIVE_ADD_REQUEST_ID_HEADER` | `false` | Whether to add `x-archive-request-id` to upstream requests. |
| `ARCHIVE_FORWARD_TIMEOUT_SECONDS` | `0` | Forwarding timeout. `0` means no total timeout. |
| `ARCHIVE_REDACT_HEADERS` | `false` | Whether sensitive headers are redacted in archive files; the example config sets this to `true`. |
| `ARCHIVE_STREAM_FLUSH_CHUNKS` | `32` | Flush streaming archives after this many chunks. |
| `ARCHIVE_STREAM_FLUSH_BYTES` | `262144` | Flush streaming archives after this many buffered bytes. |
| `ARCHIVE_SHUTDOWN_DRAIN_SECONDS` | `5` | Seconds to wait for background archive writes during normal shutdown. |
| `ARCHIVE_QUEUE_MAXSIZE` | `20000` | Maximum background archive queue length. When full, best-effort archives may be dropped to protect request latency. |
| `ARCHIVE_WRITER_WORKERS` | `4` | Background archive writer count inside each uvicorn worker. |
| `ARCHIVE_INDEX_MODE` | `worker` | `worker` writes `index.<pid>.jsonl` for multi-worker safety; `shared` writes the older shared `index.jsonl`. |

### LiteLLM

| Variable | Description |
| --- | --- |
| `LITELLM_MASTER_KEY` | Master key clients use to access local LiteLLM. |
| `UPSTREAM_API_KEY` | API key for the upstream OpenAI / Anthropic compatible service. |
| `UPSTREAM_OPENAI_BASE` | OpenAI-compatible base URL, usually ending with `/v1`. |
| `UPSTREAM_ANTHROPIC_BASE` | Anthropic-compatible base URL, usually without `/v1`. |

`litellm-config.example.yaml` defaults to:

- Route `openai/*` to `UPSTREAM_OPENAI_BASE`.
- Route `anthropic/*` to `UPSTREAM_ANTHROPIC_BASE`.
- Read `master_key` from `LITELLM_MASTER_KEY`.

## Archive Behavior

Each request gets a request id and is written under a family, session id, and timestamp based directory. Typical layout:

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

Common files:

- `*-req.json`: request metadata and body.
- `*-headers.json`: client request headers, headers forwarded to LiteLLM, and upstream response headers.
- `*-res.json`: non-streaming response body, or streaming metadata, summary, usage, errors, and reconstructed text.
- `*-chunks.jsonl`: streaming requests only, one response chunk per line.
- `index.<pid>.jsonl`: index rows with model, status code, usage, file paths, and `archive_status`.

Archive writes favor the request path. Successful non-streaming archives run through a background queue. Critical upstream-error records write inline when the queue is full. Streaming chunks are written while forwarding and flushed in batches by chunk count or byte count. Under extreme backlog, best-effort archives may be dropped; `/_archive/stats` reports this through `dropped` and `dropped_best_effort`.

## Management Endpoints

```bash
curl http://127.0.0.1:56789/_archive/health
curl http://127.0.0.1:56789/_archive/stats
```

`/_archive/stats` includes:

- `index_mode`, `queue_maxsize`, `writer_workers`: active archive configuration.
- `archive_queue.enqueued`, `completed`, `failed`: background job counters.
- `archive_queue.queued`, `writers`: current backlog and writer count.
- `archive_queue.dropped`, `dropped_best_effort`: archives dropped because the queue was full or unavailable.
- `archive_queue.overflow_inline`: non-best-effort jobs written inline after queue overflow.

## Monthly Export

Create a local tool config first:

```bash
cp archive-tools.example.yaml archive-tools.yaml
```

Edit `archive-tools.yaml` with your New API pricing URL, archive path, and export path. The file is ignored by Git by default.

Regular run:

```bash
python3 tools/fetch_newapi_pricing.py
python3 tools/archive_monthly_export.py
```

Export a specific month:

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

Outputs include `summary.json`, `by_model.csv`, `by_model.json`, `missing_pricing.csv`, merged JSONL parts, and raw archive ZIP parts. The exporter reads `index*.jsonl` by default; use `--walk` to scan `*-res.json` files instead.

`monthly_export.month` supports:

- `previous`: previous UTC month, recommended for scheduled monthly exports.
- `current`: current UTC month.
- `YYYY-MM`: fixed month, for example `2026-04`.

## Benchmarking

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

Streaming benchmark:

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

Useful fields:

- `ok_rps`: successful request throughput.
- `latency_ms.p95` / `latency_ms.p99`: tail latency.
- `first_byte_ms.p95`: streaming first-byte latency.
- `bytes_per_second` / `mbit_per_second`: response read throughput.
- `comparison_target_over_compare`: proxy versus direct ratios.

Check `/_archive/stats` before and after benchmark runs to see whether `archive_queue.queued`, `failed`, or `dropped` changes unexpectedly.

## Local Development

Run the proxy without Docker:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
UPSTREAM_BASE_URL=http://127.0.0.1:4000 ARCHIVE_ROOT=./archives uvicorn archive_proxy:app --host 0.0.0.0 --port 8000
```

Start LiteLLM separately with `litellm-config.yaml`.

## Publishing And Security

This repository is configured to keep real secrets and runtime data out of Git. Commit example files only, and do not commit:

- `.env`
- `.env.litellm`
- `litellm-config.yaml`
- `archive-tools.yaml`
- `archives/`
- `tools/monthly_exports/`
- `tools/newapi_pricing.json`

Production notes:

- Archives may contain user prompts, model outputs, and business data. Restrict access to `archives/`.
- `ARCHIVE_REDACT_HEADERS=true` redacts headers only. It does not redact request or response bodies.
- If the proxy is exposed publicly, put it behind Nginx, Caddy, or an API gateway with TLS, access control, and rate limits.
- `archives/` grows continuously. Plan retention, object-storage archival, or cleanup jobs.
- If real credentials were ever committed to Git history, clean the history before publishing and rotate exposed credentials.

## License

This project is open source under the [MIT License](./LICENSE).

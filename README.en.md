# archive-proxy

[中文](./README.md) | English

`archive-proxy` is a transparent archive proxy in front of LiteLLM. Clients keep using OpenAI / Anthropic compatible APIs through LiteLLM, while this proxy stores request bodies, response bodies, headers, streaming chunks, usage data, and reconstructed text summaries for auditing, billing reconciliation, monthly exports, and debugging.

Default flow:

```text
client -> archive-proxy:8000 -> LiteLLM:4000 -> upstream OpenAI / Anthropic compatible service
```

The included Docker Compose file exposes the proxy on `127.0.0.1:56789` and keeps LiteLLM internal to the Compose network.

## Features

- Transparent forwarding for LiteLLM HTTP requests.
- Automatic OpenAI / Anthropic request family detection.
- Archives both non-streaming responses and SSE streaming chunks.
- Session-aware directory layout via `x-archive-session-id`, `x-session-id`, `conversation_id`, and related fields.
- Writes `index.jsonl` for search and monthly reporting.
- Optional header redaction for `Authorization`, `x-api-key`, cookies, and related sensitive headers.
- Includes New API pricing fetch and monthly export scripts.

## Security Before Publishing

This repository is configured to keep real secrets and runtime data out of Git. Commit example files only, and do not commit:

- `.env`
- `.env.litellm`
- `litellm-config.yaml`
- `archives/`
- `tools/monthly_exports/`
- `tools/newapi_pricing.json`

If any of these files were already committed to Git history, clean the history before publishing and rotate exposed credentials.

## Quick Start

### 1. Prepare config files

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

For open source and production usage, keep header redaction enabled in `.env`:

```env
ARCHIVE_REDACT_HEADERS=true
```

### 2. Start services

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
curl http://127.0.0.1:56789/_archive/health
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

### archive-proxy environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `UPSTREAM_BASE_URL` | `http://litellm:4000` | Upstream LiteLLM base URL. Keep the default in Docker Compose. |
| `ARCHIVE_ROOT` | `/data/archive` | Directory for archive files. |
| `DEFAULT_FAMILY` | `openai` | Fallback request family when auto-detection is inconclusive. |
| `ARCHIVE_MAX_INLINE_BODY_BYTES` | `52428800` | Maximum non-streaming body size stored inline. Larger bodies store metadata only. |
| `ARCHIVE_ADD_REQUEST_ID_HEADER` | `false` | Whether to add `x-archive-request-id` to upstream requests. |
| `ARCHIVE_FORWARD_TIMEOUT_SECONDS` | `0` | Forwarding timeout. `0` means no total timeout. |
| `ARCHIVE_REDACT_HEADERS` | `true` in example | Whether sensitive headers are redacted in archived header files. |

### LiteLLM environment variables

| Variable | Description |
| --- | --- |
| `LITELLM_MASTER_KEY` | Master key used by clients to access local LiteLLM. |
| `UPSTREAM_API_KEY` | API key for the upstream OpenAI / Anthropic compatible service. |
| `UPSTREAM_OPENAI_BASE` | OpenAI-compatible base URL, usually ending with `/v1`. |
| `UPSTREAM_ANTHROPIC_BASE` | Anthropic-compatible base URL, usually without `/v1`. |

`litellm-config.example.yaml` routes:

- `openai/*` to `UPSTREAM_OPENAI_BASE`
- `anthropic/*` to `UPSTREAM_ANTHROPIC_BASE`
- `master_key` from `LITELLM_MASTER_KEY`

## Archive Layout

```text
archives/
  index.jsonl
  openai/
    index.jsonl
    no_session/
    session/
  anthropic/
    index.jsonl
    no_session/
    session/
```

Each request usually creates:

- `*-req.json`: request metadata and body.
- `*-headers.json`: client request headers, upstream request headers, and upstream response headers.
- `*-res.json`: response body or streaming chunks, plus usage, error details, and reconstructed text summary.

## Monthly Export

For one-command monthly usage, create a local config first:

```bash
cp archive-tools.example.yaml archive-tools.yaml
```

Edit `archive-tools.yaml` with your New API pricing URL, archive path, export path, and other local defaults. This file is ignored by Git and should not be committed.

After that, run:

```bash
python3 tools/fetch_newapi_pricing.py
python3 tools/archive_monthly_export.py
```

`monthly_export.month` supports:

- `previous`: previous UTC month, recommended for monthly exports.
- `current`: current UTC month.
- `YYYY-MM`: fixed month, for example `2026-04`.

You can still override config values with CLI flags when needed. Fetch a pricing file:

```bash
python3 tools/fetch_newapi_pricing.py \
  --url https://your-new-api.example.com/pricing \
  --out tools/newapi_pricing.json
```

Export reports and archive packages for a month:

```bash
python3 tools/archive_monthly_export.py \
  --archive-root ./archives \
  --pricing ./tools/newapi_pricing.json \
  --month 2026-04 \
  --out-dir ./tools/monthly_exports/2026-04 \
  --mode all
```

The output includes per-model JSON / CSV summaries, missing-pricing reports, merged JSONL parts, and ZIP parts. Export outputs are ignored by Git by default.

## Local Development

Run the proxy without Docker:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
UPSTREAM_BASE_URL=http://127.0.0.1:4000 ARCHIVE_ROOT=./archives uvicorn archive_proxy:app --host 0.0.0.0 --port 8000
```

Start LiteLLM separately with `litellm-config.yaml`.

## Notes

- Archives may contain user prompts, model outputs, and business data. Restrict access to `archives/` in production.
- `ARCHIVE_REDACT_HEADERS=true` redacts headers only. It does not redact request or response bodies.
- If the proxy is exposed publicly, put it behind Nginx, Caddy, or an API gateway with TLS, access control, and rate limits.
- `archives/` grows continuously. Plan retention, object-storage archival, or cleanup jobs.

## License

This project is open source under the [MIT License](./LICENSE).

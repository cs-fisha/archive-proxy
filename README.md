# archive-proxy

中文 | [English](./README.en.md)

`archive-proxy` 是一个放在 LiteLLM 前面的透明归档代理。客户端仍然使用 OpenAI / Anthropic 兼容接口请求 LiteLLM，代理会把请求、响应、响应头、流式 chunk、usage 和可重建文本摘要落盘，方便后续审计、对账、月度导出和问题排查。

默认链路：

```text
client -> archive-proxy:8000 -> LiteLLM:4000 -> 上游 OpenAI / Anthropic 兼容服务
```

Docker Compose 中默认把代理暴露在本机 `127.0.0.1:56789`，LiteLLM 只在容器网络内开放。

## 功能

- 透明转发 LiteLLM 支持的 HTTP 请求。
- 自动区分 OpenAI / Anthropic 请求族。
- 支持普通响应和 SSE 流式响应归档。
- 按 session 分目录保存，支持 `x-archive-session-id`、`x-session-id`、`conversation_id` 等字段。
- 生成 `index.jsonl`，便于后续检索和月度统计。
- 可选 header 脱敏，避免归档里保存 `Authorization`、`x-api-key`、Cookie 等敏感头。
- 附带 New API 价格抓取和月度导出脚本。

## 开源前安全说明

本仓库默认不会提交真实密钥和运行数据。请只提交 example 文件，不要提交下列本地文件：

- `.env`
- `.env.litellm`
- `litellm-config.yaml`
- `archives/`
- `tools/monthly_exports/`
- `tools/newapi_pricing.json`

如果这些文件曾经进入过 Git 历史，需要在公开仓库前清理历史并轮换已经泄露的密钥。

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

编辑 `.env` 时建议开源和生产环境都启用 header 脱敏：

```env
ARCHIVE_REDACT_HEADERS=true
```

### 2. 启动

```bash
docker compose up -d --build
```

查看状态：

```bash
docker compose ps
curl http://127.0.0.1:56789/_archive/health
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

## 配置说明

### archive-proxy 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `UPSTREAM_BASE_URL` | `http://litellm:4000` | LiteLLM 上游地址。Docker Compose 内保持默认即可。 |
| `ARCHIVE_ROOT` | `/data/archive` | 归档文件保存目录。 |
| `DEFAULT_FAMILY` | `openai` | 无法识别请求族时使用的默认族，可选 `openai` 或 `anthropic`。 |
| `ARCHIVE_MAX_INLINE_BODY_BYTES` | `52428800` | 单个非流式 body 最大内联保存大小，超过后只保存元数据。 |
| `ARCHIVE_ADD_REQUEST_ID_HEADER` | `false` | 是否向上游追加 `x-archive-request-id`。 |
| `ARCHIVE_FORWARD_TIMEOUT_SECONDS` | `0` | 转发总超时，`0` 表示不设置总超时。 |
| `ARCHIVE_REDACT_HEADERS` | `true` in example | 是否在归档 header 时脱敏敏感头。 |

### LiteLLM 环境变量

| 变量 | 说明 |
| --- | --- |
| `LITELLM_MASTER_KEY` | 客户端访问本地 LiteLLM 的 master key。 |
| `UPSTREAM_API_KEY` | 上游 OpenAI / Anthropic 兼容服务的 API key。 |
| `UPSTREAM_OPENAI_BASE` | OpenAI 兼容接口 base URL，通常以 `/v1` 结尾。 |
| `UPSTREAM_ANTHROPIC_BASE` | Anthropic 兼容接口 base URL，通常不带 `/v1`。 |

`litellm-config.example.yaml` 默认配置了：

- `openai/*` 转发到 `UPSTREAM_OPENAI_BASE`
- `anthropic/*` 转发到 `UPSTREAM_ANTHROPIC_BASE`
- `master_key` 从 `LITELLM_MASTER_KEY` 读取

## 归档目录结构

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

每次请求通常会生成三类文件：

- `*-req.json`：请求元数据和请求体。
- `*-headers.json`：客户端请求头、转发给上游的请求头、上游响应头。
- `*-res.json`：响应体或流式 chunks，以及 usage、错误、重建文本摘要。

## 月度导出

先准备价格文件：

```bash
python3 tools/fetch_newapi_pricing.py \
  --url https://your-new-api.example.com/pricing \
  --out tools/newapi_pricing.json
```

导出某个月的统计和归档包：

```bash
python3 tools/archive_monthly_export.py \
  --archive-root ./archives \
  --pricing ./tools/newapi_pricing.json \
  --month 2026-04 \
  --out-dir ./tools/monthly_exports/2026-04 \
  --mode all
```

导出目录会包含按模型统计的 JSON / CSV、缺失价格列表、合并后的 JSONL 和 ZIP 分片。导出结果默认不提交到 Git。

## 本地开发

不使用 Docker 时可以直接运行代理：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
UPSTREAM_BASE_URL=http://127.0.0.1:4000 ARCHIVE_ROOT=./archives uvicorn archive_proxy:app --host 0.0.0.0 --port 8000
```

LiteLLM 可参考官方方式启动，并使用 `litellm-config.yaml` 作为配置文件。

## 注意事项

- 归档内容可能包含用户提示词、模型输出和业务数据，生产环境请限制 `archives/` 目录权限。
- `ARCHIVE_REDACT_HEADERS=true` 只脱敏 header，不会脱敏请求体或响应体。
- 如果代理对公网开放，建议在前面加 Nginx / Caddy / API Gateway，并配置 TLS、访问控制和限流。
- `archives/` 会持续增长，需要结合业务保留周期做日志轮转、对象存储归档或定期清理。

## 许可证

本项目基于 [MIT License](./LICENSE) 开源。

import asyncio
import base64
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


UPSTREAM_BASE_URL = os.environ.get("UPSTREAM_BASE_URL", "http://litellm:4000").rstrip("/")
ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "/data/archive"))
DEFAULT_FAMILY = os.environ.get("DEFAULT_FAMILY", "openai")
ARCHIVE_MAX_INLINE_BODY_BYTES = int(os.environ.get("ARCHIVE_MAX_INLINE_BODY_BYTES", str(50 * 1024 * 1024)))
ARCHIVE_ADD_REQUEST_ID_HEADER = os.environ.get("ARCHIVE_ADD_REQUEST_ID_HEADER", "false").lower() == "true"
ARCHIVE_FORWARD_TIMEOUT_SECONDS = float(os.environ.get("ARCHIVE_FORWARD_TIMEOUT_SECONDS", "0"))  # 0 = no total timeout
ARCHIVE_REDACT_HEADERS = os.environ.get("ARCHIVE_REDACT_HEADERS", "false").lower() == "true"
ARCHIVE_STREAM_FLUSH_CHUNKS = max(1, int(os.environ.get("ARCHIVE_STREAM_FLUSH_CHUNKS", "32")))
ARCHIVE_STREAM_FLUSH_BYTES = max(1, int(os.environ.get("ARCHIVE_STREAM_FLUSH_BYTES", str(256 * 1024))))
ARCHIVE_SHUTDOWN_DRAIN_SECONDS = max(0.0, float(os.environ.get("ARCHIVE_SHUTDOWN_DRAIN_SECONDS", "5")))
ARCHIVE_QUEUE_MAXSIZE = max(1, int(os.environ.get("ARCHIVE_QUEUE_MAXSIZE", "20000")))
ARCHIVE_WRITER_WORKERS = max(1, int(os.environ.get("ARCHIVE_WRITER_WORKERS", "4")))
ARCHIVE_INDEX_MODE = os.environ.get("ARCHIVE_INDEX_MODE", "worker").strip().lower()
if ARCHIVE_INDEX_MODE not in {"worker", "shared"}:
    ARCHIVE_INDEX_MODE = "worker"

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}

SENSITIVE_HEADER_NAMES = {
    "authorization",
    "x-api-key",
    "api-key",
    "anthropic-api-key",
    "openai-api-key",
    "cookie",
    "set-cookie",
}

SESSION_HEADER_CANDIDATES = [
    "x-archive-session-id",
    "x-session-id",
    "x-conversation-id",
    "x-chat-id",
    "x-thread-id",
    "session-id",
    "conversation-id",
]

SESSION_BODY_CANDIDATES = [
    "session_id",
    "conversation_id",
    "thread_id",
    "chat_id",
]

app = FastAPI(title="archive-proxy", docs_url=None, redoc_url=None)
_client: Optional[httpx.AsyncClient] = None
_archive_queue: Optional[asyncio.Queue] = None
_archive_writer_tasks: set[asyncio.Task] = set()
_archive_stats = {
    "enqueued": 0,
    "completed": 0,
    "failed": 0,
    "cancelled": 0,
    "dropped": 0,
    "dropped_best_effort": 0,
    "overflow_inline": 0,
    "shutdown_drained": 0,
    "shutdown_pending": 0,
}


ArchiveJob = Tuple[Callable[..., Any], Tuple[Any, ...]]


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(value: str, max_len: int = 140) -> str:
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9._=@+-]+", "_", value)
    value = value.strip("._-") or "empty"
    return value[:max_len]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _to_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)


def iter_header_str(headers: Iterable[Tuple[Any, Any]]) -> Iterable[Tuple[str, str]]:
    for k, v in headers:
        yield _to_str(k), _to_str(v)


def lower_header_dict(headers: Iterable[Tuple[Any, Any]]) -> Dict[str, str]:
    return {k.lower(): v for k, v in iter_header_str(headers)}


def normalize_headers(headers: Iterable[Tuple[Any, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in iter_header_str(headers):
        if ARCHIVE_REDACT_HEADERS and k.lower() in SENSITIVE_HEADER_NAMES:
            out[k] = "<redacted>"
        else:
            out[k] = v
    return out


def filter_request_headers(headers: Iterable[Tuple[Any, Any]], request_id: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in iter_header_str(headers):
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS:
            continue
        out[k] = v
    if ARCHIVE_ADD_REQUEST_ID_HEADER:
        out["x-archive-request-id"] = request_id
    return out


def filter_response_headers(headers: Iterable[Tuple[Any, Any]], streaming: bool) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in iter_header_str(headers):
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS:
            continue
        # httpx may decode compressed bodies; do not pass stale encoding/length headers downstream.
        if lk in {"content-length", "content-encoding"}:
            continue
        if streaming and lk == "content-length":
            continue
        out[k] = v
    return out


def parse_json_maybe(raw: bytes, content_type: str = "") -> Optional[Any]:
    if not raw:
        return None
    should_try = "json" in content_type.lower() or raw[:1] in (b"{", b"[")
    if not should_try:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def body_for_archive(raw: bytes, content_type: str = "") -> Dict[str, Any]:
    meta = {
        "sha256": sha256_bytes(raw),
        "size_bytes": len(raw),
        "content_type": content_type,
    }
    if len(raw) > ARCHIVE_MAX_INLINE_BODY_BYTES:
        return {
            "meta": meta,
            "encoding": "omitted_too_large",
            "truncated": True,
            "note": f"Body is larger than ARCHIVE_MAX_INLINE_BODY_BYTES={ARCHIVE_MAX_INLINE_BODY_BYTES}; only metadata was stored.",
        }

    parsed = parse_json_maybe(raw, content_type)
    if parsed is not None:
        return {"meta": meta, "encoding": "json", "data": parsed}

    try:
        return {"meta": meta, "encoding": "utf-8", "data": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"meta": meta, "encoding": "base64", "data": base64.b64encode(raw).decode("ascii")}


def first_nonempty(*values: Any) -> Optional[str]:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def dig(obj: Any, path: List[str]) -> Optional[Any]:
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def extract_model(body_json: Optional[Any]) -> Optional[str]:
    if isinstance(body_json, dict):
        return first_nonempty(body_json.get("model"), dig(body_json, ["message", "model"]))
    return None


def extract_session_id(headers_lc: Dict[str, str], query: Dict[str, str], body_json: Optional[Any]) -> Optional[str]:
    for key in SESSION_HEADER_CANDIDATES:
        if key in headers_lc and headers_lc[key].strip():
            return headers_lc[key].strip()

    for key in SESSION_BODY_CANDIDATES:
        if key in query and str(query[key]).strip():
            return str(query[key]).strip()

    if isinstance(body_json, dict):
        for key in SESSION_BODY_CANDIDATES:
            value = body_json.get(key)
            if value:
                return str(value)
        metadata = body_json.get("metadata")
        if isinstance(metadata, dict):
            for key in SESSION_BODY_CANDIDATES:
                value = metadata.get(key)
                if value:
                    return str(value)
            # Some tools use metadata.user_id. It is not a true session, so we only use it
            # when the caller explicitly enables it via x-archive-session-id instead.
    return None


def guess_family(path: str, headers_lc: Dict[str, str], body_json: Optional[Any]) -> str:
    model = (extract_model(body_json) or "").lower()
    p = path.lower()

    if "anthropic-version" in headers_lc or "anthropic-beta" in headers_lc:
        return "anthropic"
    if model.startswith("anthropic/"):
        return "anthropic"
    if "/messages" in p or p.endswith("/complete") or "/v1/complete" in p:
        return "anthropic"

    if model.startswith("openai/"):
        return "openai"
    if any(token in p for token in ["/chat/completions", "/embeddings", "/models", "/responses", "/completions"]):
        return "openai"

    return DEFAULT_FAMILY if DEFAULT_FAMILY in {"openai", "anthropic"} else "openai"


def ensure_dirs() -> None:
    for family in ("openai", "anthropic"):
        (ARCHIVE_ROOT / family / "session").mkdir(parents=True, exist_ok=True)
        (ARCHIVE_ROOT / family / "no_session").mkdir(parents=True, exist_ok=True)
    ARCHIVE_ROOT.mkdir(parents=True, exist_ok=True)


def find_or_create_session_dir(family: str, session_id: str, ts_prefix: str) -> Path:
    base = ARCHIVE_ROOT / family / "session"
    sid = safe_name(session_id)
    d = base / sid
    d.mkdir(parents=True, exist_ok=True)

    meta = d / "session.json"
    if not meta.exists():
        write_json(meta, {
            "session_id": session_id,
            "safe_session_id": sid,
            "family": family,
            "created_ts": ts_prefix,
            "created_iso_ts": iso_now(),
        })
    return d


def make_record_paths(family: str, session_id: Optional[str], ts_prefix: str, request_id: str) -> Dict[str, Path]:
    if session_id:
        root = find_or_create_session_dir(family, session_id, ts_prefix)
    else:
        root = ARCHIVE_ROOT / family / "no_session"
        root.mkdir(parents=True, exist_ok=True)
    prefix = f"{ts_prefix}_{request_id}"
    return {
        "root": root,
        "req": root / f"{prefix}-req.json",
        "res": root / f"{prefix}-res.json",
        "chunks": root / f"{prefix}-chunks.jsonl",
        "headers": root / f"{prefix}-headers.json",
    }


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    tmp.replace(path)


def append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def append_index_sync(family: str, record: Dict[str, Any]) -> None:
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    if ARCHIVE_INDEX_MODE == "worker":
        pid = os.getpid()
        paths = [
            ARCHIVE_ROOT / f"index.{pid}.jsonl",
            ARCHIVE_ROOT / family / f"index.{pid}.jsonl",
        ]
    else:
        paths = [ARCHIVE_ROOT / "index.jsonl", ARCHIVE_ROOT / family / "index.jsonl"]

    for path in paths:
        append_line(path, line)


def write_archive_files(files: Iterable[Tuple[Path, Any]], family: str, index_record: Optional[Dict[str, Any]]) -> None:
    for path, obj in files:
        write_json(path, obj)
    if index_record is not None:
        append_index_sync(family, index_record)


async def archive_worker() -> None:
    assert _archive_queue is not None
    while True:
        job = await _archive_queue.get()
        if job is None:
            _archive_queue.task_done()
            break
        func, args = job
        try:
            await asyncio.to_thread(func, *args)
        except asyncio.CancelledError:
            _archive_stats["cancelled"] += 1
            raise
        except Exception as exc:
            _archive_stats["failed"] += 1
            print(f"archive background job failed: {type(exc).__name__}: {exc}", flush=True)
        else:
            _archive_stats["completed"] += 1
        finally:
            _archive_queue.task_done()


async def enqueue_archive_job(func: Callable[..., Any], *args: Any, best_effort: bool = True) -> bool:
    if _archive_queue is None:
        _archive_stats["dropped"] += 1
        return False
    job: ArchiveJob = (func, args)
    try:
        _archive_queue.put_nowait(job)
    except asyncio.QueueFull:
        if best_effort:
            _archive_stats["dropped"] += 1
            _archive_stats["dropped_best_effort"] += 1
            return False

        _archive_stats["overflow_inline"] += 1
        await asyncio.to_thread(func, *args)
    else:
        _archive_stats["enqueued"] += 1
    return True


async def drain_archive_queue(timeout: float) -> None:
    if _archive_queue is None:
        _archive_stats["shutdown_drained"] = 0
        _archive_stats["shutdown_pending"] = 0
        return

    before = _archive_queue.qsize()
    try:
        await asyncio.wait_for(_archive_queue.join(), timeout=timeout)
    except asyncio.TimeoutError:
        _archive_stats["shutdown_drained"] = max(0, before - _archive_queue.qsize())
        _archive_stats["shutdown_pending"] = _archive_queue.qsize()
        print(f"archive shutdown drain timed out with {_archive_queue.qsize()} queued job(s)", flush=True)
    else:
        _archive_stats["shutdown_drained"] = before
        _archive_stats["shutdown_pending"] = 0


def parse_sse_from_buffer(buffer: str) -> Tuple[List[Dict[str, Any]], str]:
    # Normalize CRLF to LF for SSE parsing. Raw chunks are still archived separately.
    buffer = buffer.replace("\r\n", "\n")
    events: List[Dict[str, Any]] = []
    while "\n\n" in buffer:
        block, buffer = buffer.split("\n\n", 1)
        if not block.strip():
            continue
        event_name = None
        data_lines: List[str] = []
        other_lines: List[str] = []
        for line in block.split("\n"):
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_name = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:"):].lstrip())
            else:
                other_lines.append(line)
        data_text = "\n".join(data_lines)
        data_json = None
        if data_text and data_text != "[DONE]":
            try:
                data_json = json.loads(data_text)
            except Exception:
                data_json = None
        events.append({
            "event": event_name,
            "data_text": data_text,
            "data_json": data_json,
            "other_lines": other_lines,
        })
    return events, buffer


class Reconstructor:
    def __init__(self, family: str, request_model: Optional[str] = None) -> None:
        self.family = family
        self.model = request_model
        self.usage: Optional[Dict[str, Any]] = None
        self.content_parts: List[str] = []
        self.reasoning_parts: List[str] = []
        self.tool_events: List[Any] = []
        self.finish_reason: Optional[str] = None
        self.error: Optional[Any] = None
        self.seen_done = False

    def feed_sse_event(self, event: Dict[str, Any]) -> None:
        data = event.get("data_json")
        data_text = event.get("data_text")
        if data_text == "[DONE]":
            self.seen_done = True
            return
        if not isinstance(data, dict):
            return

        if "error" in data:
            self.error = data.get("error")

        if self.family == "anthropic":
            self._feed_anthropic(data, event.get("event"))
        else:
            self._feed_openai(data)

    def _feed_openai(self, data: Dict[str, Any]) -> None:
        if data.get("model"):
            self.model = data.get("model")
        if isinstance(data.get("usage"), dict):
            self.usage = data.get("usage")
        choices = data.get("choices")
        if isinstance(choices, list):
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                delta = choice.get("delta") or {}
                if isinstance(delta, dict):
                    content = delta.get("content")
                    if isinstance(content, str):
                        self.content_parts.append(content)
                    reasoning = first_nonempty(delta.get("reasoning_content"), delta.get("reasoning"))
                    if reasoning:
                        self.reasoning_parts.append(reasoning)
                    if delta.get("tool_calls") is not None:
                        self.tool_events.append(delta.get("tool_calls"))
                if choice.get("finish_reason"):
                    self.finish_reason = choice.get("finish_reason")

    def _feed_anthropic(self, data: Dict[str, Any], event_name: Optional[str]) -> None:
        typ = data.get("type") or event_name
        if typ == "message_start":
            msg = data.get("message") or {}
            if isinstance(msg, dict):
                if msg.get("model"):
                    self.model = msg.get("model")
                if isinstance(msg.get("usage"), dict):
                    self.usage = msg.get("usage")
        elif typ == "content_block_start":
            block = data.get("content_block") or {}
            if isinstance(block, dict):
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    self.content_parts.append(block.get("text"))
                elif block.get("type") in {"tool_use", "server_tool_use"}:
                    self.tool_events.append({"start": block})
        elif typ == "content_block_delta":
            delta = data.get("delta") or {}
            if isinstance(delta, dict):
                dtype = delta.get("type")
                if dtype == "text_delta" and isinstance(delta.get("text"), str):
                    self.content_parts.append(delta.get("text"))
                elif dtype in {"thinking_delta", "signature_delta"} and isinstance(delta.get("thinking"), str):
                    self.reasoning_parts.append(delta.get("thinking"))
                elif dtype == "thinking_delta" and isinstance(delta.get("text"), str):
                    self.reasoning_parts.append(delta.get("text"))
                elif dtype == "input_json_delta":
                    self.tool_events.append({"partial_json": delta.get("partial_json")})
        elif typ == "message_delta":
            delta = data.get("delta") or {}
            if isinstance(delta, dict) and delta.get("stop_reason"):
                self.finish_reason = delta.get("stop_reason")
            if isinstance(data.get("usage"), dict):
                if isinstance(self.usage, dict):
                    merged = dict(self.usage)
                    merged.update(data.get("usage"))
                    self.usage = merged
                else:
                    self.usage = data.get("usage")
        elif typ == "error":
            self.error = data.get("error") or data

    def feed_nonstream_body(self, body_json: Any) -> None:
        if not isinstance(body_json, dict):
            return
        if body_json.get("model"):
            self.model = body_json.get("model")
        if isinstance(body_json.get("usage"), dict):
            self.usage = body_json.get("usage")
        if "error" in body_json:
            self.error = body_json.get("error")

        if self.family == "anthropic":
            content = body_json.get("content")
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "text" and isinstance(part.get("text"), str):
                        self.content_parts.append(part.get("text"))
                    elif part.get("type") in {"tool_use", "server_tool_use"}:
                        self.tool_events.append(part)
            if body_json.get("stop_reason"):
                self.finish_reason = body_json.get("stop_reason")
        else:
            choices = body_json.get("choices")
            if isinstance(choices, list):
                for choice in choices:
                    if not isinstance(choice, dict):
                        continue
                    message = choice.get("message") or {}
                    if isinstance(message, dict):
                        content = message.get("content")
                        if isinstance(content, str):
                            self.content_parts.append(content)
                        elif isinstance(content, list):
                            # Responses API or multimodal-ish content. Keep text leaves.
                            for part in content:
                                if isinstance(part, dict) and isinstance(part.get("text"), str):
                                    self.content_parts.append(part.get("text"))
                        reasoning = first_nonempty(message.get("reasoning_content"), message.get("reasoning"))
                        if reasoning:
                            self.reasoning_parts.append(reasoning)
                        if message.get("tool_calls") is not None:
                            self.tool_events.append(message.get("tool_calls"))
                    if choice.get("finish_reason"):
                        self.finish_reason = choice.get("finish_reason")

    def summary(self) -> Dict[str, Any]:
        content = "".join(self.content_parts)
        reasoning = "".join(self.reasoning_parts)
        return {
            "model": self.model,
            "usage": self.usage,
            "finish_reason": self.finish_reason,
            "has_error": self.error is not None,
            "error": self.error,
            "reconstructed": {
                "content_text": content,
                "content_text_sha256": sha256_bytes(content.encode("utf-8")) if content else None,
                "content_text_chars": len(content),
                "reasoning_text": reasoning if reasoning else None,
                "reasoning_text_sha256": sha256_bytes(reasoning.encode("utf-8")) if reasoning else None,
                "tool_event_count": len(self.tool_events),
                "tool_events": self.tool_events[:50],
            },
        }


class StreamingArchiveWriter:
    def __init__(self, chunks_path: Path, meta: Dict[str, Any], reconstructor: Reconstructor) -> None:
        self.chunks_path = chunks_path
        self.meta = meta
        self.reconstructor = reconstructor
        self.count = 0
        self.total_bytes = 0
        self.unflushed_chunks = 0
        self.unflushed_bytes = 0
        self.sse_buffer = ""
        self.file = None

    def start(self) -> None:
        self.chunks_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.chunks_path.open("w", encoding="utf-8")

    def add_chunk(self, chunk: bytes) -> None:
        assert self.file is not None
        self.count += 1
        self.total_bytes += len(chunk)
        text = None
        text_truncated = False
        try:
            text = chunk.decode("utf-8")
            if len(text) > 200_000:
                text = text[:200_000]
                text_truncated = True
        except UnicodeDecodeError:
            text = None

        sse_events: List[Dict[str, Any]] = []
        if text is not None:
            sse_events, self.sse_buffer = parse_sse_from_buffer(self.sse_buffer + text)
            for event in sse_events:
                self.reconstructor.feed_sse_event(event)

        obj: Dict[str, Any] = {
            "seq": self.count,
            "ts": iso_now(),
            "size_bytes": len(chunk),
            "sha256": sha256_bytes(chunk),
            "text": text,
            "text_truncated": text_truncated,
            "sse_events": sse_events,
        }
        if text is None:
            obj["base64"] = base64.b64encode(chunk).decode("ascii")

        self.file.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.unflushed_chunks += 1
        self.unflushed_bytes += len(chunk)
        if self.unflushed_chunks >= ARCHIVE_STREAM_FLUSH_CHUNKS or self.unflushed_bytes >= ARCHIVE_STREAM_FLUSH_BYTES:
            self.flush()

    def flush(self) -> None:
        assert self.file is not None
        self.file.flush()
        self.unflushed_chunks = 0
        self.unflushed_bytes = 0

    def finish(self, status: str = "complete") -> Dict[str, Any]:
        assert self.file is not None
        summary = self.reconstructor.summary()
        tail = {
            "stream_archive_status": status,
            "archive_status": "complete" if status == "complete" else "partial",
            "chunk_count": self.count,
            "total_stream_bytes": self.total_bytes,
            "unparsed_sse_buffer_chars": len(self.sse_buffer),
            **summary,
        }
        self.flush()
        self.file.close()
        return tail


def make_upstream_url(path: str, raw_query: bytes) -> str:
    url = f"{UPSTREAM_BASE_URL}/{path}"
    if raw_query:
        url += "?" + raw_query.decode("latin-1")
    return url


@app.on_event("startup")
async def startup() -> None:
    global _archive_queue, _client
    ensure_dirs()
    _archive_queue = asyncio.Queue(maxsize=ARCHIVE_QUEUE_MAXSIZE)
    for _ in range(ARCHIVE_WRITER_WORKERS):
        task = asyncio.create_task(archive_worker())
        _archive_writer_tasks.add(task)
    timeout = None if ARCHIVE_FORWARD_TIMEOUT_SECONDS <= 0 else ARCHIVE_FORWARD_TIMEOUT_SECONDS
    _client = httpx.AsyncClient(timeout=timeout, follow_redirects=False)


@app.on_event("shutdown")
async def shutdown() -> None:
    global _archive_queue, _client
    await drain_archive_queue(ARCHIVE_SHUTDOWN_DRAIN_SECONDS)
    if _archive_queue is not None:
        for _ in _archive_writer_tasks:
            await _archive_queue.put(None)
        if _archive_writer_tasks:
            await asyncio.gather(*_archive_writer_tasks, return_exceptions=True)
        _archive_writer_tasks.clear()
        _archive_queue = None
    if _client is not None:
        await _client.aclose()
        _client = None


@app.get("/_archive/health")
async def health() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "upstream_base_url": UPSTREAM_BASE_URL,
        "archive_root": str(ARCHIVE_ROOT),
    })


@app.get("/_archive/stats")
async def stats() -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "pid": os.getpid(),
        "archive_root": str(ARCHIVE_ROOT),
        "index_mode": ARCHIVE_INDEX_MODE,
        "stream_flush_chunks": ARCHIVE_STREAM_FLUSH_CHUNKS,
        "stream_flush_bytes": ARCHIVE_STREAM_FLUSH_BYTES,
        "shutdown_drain_seconds": ARCHIVE_SHUTDOWN_DRAIN_SECONDS,
        "queue_maxsize": ARCHIVE_QUEUE_MAXSIZE,
        "writer_workers": ARCHIVE_WRITER_WORKERS,
        "archive_queue": {
            **_archive_stats,
            "queued": _archive_queue.qsize() if _archive_queue is not None else 0,
            "writers": len(_archive_writer_tasks),
        },
    })


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_all(full_path: str, request: Request) -> Response:
    assert _client is not None

    request_id = uuid.uuid4().hex[:12]
    ts_prefix = utc_ts()
    started = time.perf_counter()
    raw_body = await request.body()
    headers_lc = lower_header_dict(request.headers.raw)
    content_type = request.headers.get("content-type", "")
    body_json = parse_json_maybe(raw_body, content_type)

    family = guess_family("/" + full_path, headers_lc, body_json)
    session_id = extract_session_id(headers_lc, dict(request.query_params), body_json)
    paths = make_record_paths(family, session_id, ts_prefix, request_id)
    upstream_url = make_upstream_url(full_path, request.url.query.encode("latin-1"))
    forwarded_headers = filter_request_headers(request.headers.raw, request_id)
    client_headers_archive = normalize_headers(request.headers.raw)
    upstream_request_headers_archive = normalize_headers(forwarded_headers.items())

    model_req = extract_model(body_json)
    stream_requested = bool(isinstance(body_json, dict) and body_json.get("stream") is True)

    req_record = {
        "meta": {
            "request_id": request_id,
            "ts": ts_prefix,
            "iso_ts": iso_now(),
            "family": family,
            "session_id": session_id,
            "has_session": session_id is not None,
            "method": request.method,
            "path": "/" + full_path,
            "query": str(request.url.query),
            "client": request.client.host if request.client else None,
            "model": model_req,
            "stream_requested": stream_requested,
            "upstream_url": upstream_url,
        },
        "body": body_for_archive(raw_body, content_type),
    }
    upstream_resp: Optional[httpx.Response] = None
    try:
        upstream_req = _client.build_request(
            request.method,
            upstream_url,
            headers=forwarded_headers,
            content=raw_body,
        )
        upstream_resp = await _client.send(upstream_req, stream=True)
    except Exception as e:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        error_obj = {"type": type(e).__name__, "message": str(e)}
        headers_record = {
            "meta": {"request_id": request_id, "ts": ts_prefix, "family": family, "session_id": session_id},
            "client_request_headers": client_headers_archive,
            "upstream_request_headers": upstream_request_headers_archive,
            "upstream_response_headers": None,
            "primary_header_source": "client_request_headers" if family == "openai" else "upstream_response_headers",
        }
        res_record = {
            "meta": {"request_id": request_id, "ts": ts_prefix, "family": family, "session_id": session_id},
            "summary": {
                "status_code": 502,
                "latency_ms": latency_ms,
                "is_stream": False,
                "archive_status": "complete",
                "has_error": True,
                "error": error_obj,
                "model": model_req,
                "usage": None,
            },
        }
        index_record = {
            "ts": ts_prefix,
            "request_id": request_id,
            "family": family,
            "session_id": session_id,
            "has_session": session_id is not None,
            "method": request.method,
            "path": "/" + full_path,
            "model": model_req,
            "status_code": 502,
            "latency_ms": latency_ms,
            "is_stream": False,
            "has_error": True,
            "error": error_obj,
            "usage": None,
            "record_dir": str(paths["root"]),
            "req_file": str(paths["req"]),
            "res_file": str(paths["res"]),
            "chunks_file": None,
            "headers_file": str(paths["headers"]),
            "archive_status": "complete",
        }
        await enqueue_archive_job(
            write_archive_files,
            [(paths["req"], req_record), (paths["headers"], headers_record), (paths["res"], res_record)],
            family,
            index_record,
            best_effort=False,
        )
        return JSONResponse({"error": "archive-proxy upstream error", "detail": error_obj}, status_code=502)

    resp_headers_lc = {k.lower(): v for k, v in upstream_resp.headers.items()}
    content_type_resp = resp_headers_lc.get("content-type", "")
    is_stream = stream_requested or "text/event-stream" in content_type_resp.lower()
    response_headers = filter_response_headers(upstream_resp.headers.items(), streaming=is_stream)

    headers_record = {
        "meta": {"request_id": request_id, "ts": ts_prefix, "family": family, "session_id": session_id},
        "client_request_headers": client_headers_archive,
        "upstream_request_headers": upstream_request_headers_archive,
        "upstream_response_headers": normalize_headers(upstream_resp.headers.items()),
        # Compatibility marker for your note: older analysis may expect OpenAI to use
        # client headers and Anthropic to use upstream headers. We store both always.
        "primary_header_source": "client_request_headers" if family == "openai" else "upstream_response_headers",
    }
    if not is_stream:
        raw_resp_body = await upstream_resp.aread()
        await upstream_resp.aclose()
        body_json_resp = parse_json_maybe(raw_resp_body, content_type_resp)
        recon = Reconstructor(family, model_req)
        recon.feed_nonstream_body(body_json_resp)
        summary = recon.summary()
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        has_error = upstream_resp.status_code >= 400 or summary.get("has_error")
        res_record = {
            "meta": {
                "request_id": request_id,
                "ts": ts_prefix,
                "iso_ts": iso_now(),
                "family": family,
                "session_id": session_id,
                "status_code": upstream_resp.status_code,
                "latency_ms": latency_ms,
                "is_stream": False,
                "archive_status": "complete",
            },
            "body": body_for_archive(raw_resp_body, content_type_resp),
            "summary": {
                "status_code": upstream_resp.status_code,
                "latency_ms": latency_ms,
                "is_stream": False,
                "archive_status": "complete",
                "has_error": has_error,
                **summary,
            },
        }
        index_record = {
            "ts": ts_prefix,
            "request_id": request_id,
            "family": family,
            "session_id": session_id,
            "has_session": session_id is not None,
            "method": request.method,
            "path": "/" + full_path,
            "model": summary.get("model") or model_req,
            "request_model": model_req,
            "status_code": upstream_resp.status_code,
            "latency_ms": latency_ms,
            "is_stream": False,
            "has_error": has_error,
            "error": summary.get("error"),
            "usage": summary.get("usage"),
            "record_dir": str(paths["root"]),
            "req_file": str(paths["req"]),
            "res_file": str(paths["res"]),
            "chunks_file": None,
            "headers_file": str(paths["headers"]),
            "archive_status": "complete",
        }
        await enqueue_archive_job(
            write_archive_files,
            [(paths["req"], req_record), (paths["headers"], headers_record), (paths["res"], res_record)],
            family,
            index_record,
            best_effort=True,
        )
        return Response(content=raw_resp_body, status_code=upstream_resp.status_code, headers=response_headers, media_type=None)

    await enqueue_archive_job(
        write_archive_files,
        [(paths["req"], req_record), (paths["headers"], headers_record)],
        family,
        None,
        best_effort=False,
    )

    recon = Reconstructor(family, model_req)
    stream_meta = {
        "request_id": request_id,
        "ts": ts_prefix,
        "iso_ts": iso_now(),
        "family": family,
        "session_id": session_id,
        "status_code": upstream_resp.status_code,
        "is_stream": True,
        "content_type": content_type_resp,
    }
    stream_writer = StreamingArchiveWriter(paths["chunks"], stream_meta, recon)
    stream_writer.start()

    async def body_iter():
        status = "complete"
        try:
            async for chunk in upstream_resp.aiter_bytes():
                if chunk:
                    stream_writer.add_chunk(chunk)
                    yield chunk
        except Exception as e:
            status = "stream_error"
            recon.error = {"type": type(e).__name__, "message": str(e)}
            raise
        finally:
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            summary = stream_writer.finish(status=status)
            await upstream_resp.aclose()
            has_error = upstream_resp.status_code >= 400 or bool(summary.get("has_error")) or status != "complete"
            res_record = {
                "meta": {
                    **stream_meta,
                    "latency_ms": latency_ms,
                    "chunks_file": str(paths["chunks"]),
                    "archive_status": summary.get("archive_status"),
                },
                "summary": {
                    "status_code": upstream_resp.status_code,
                    "latency_ms": latency_ms,
                    "is_stream": True,
                    "has_error": has_error,
                    **summary,
                },
            }
            index_record = {
                "ts": ts_prefix,
                "request_id": request_id,
                "family": family,
                "session_id": session_id,
                "has_session": session_id is not None,
                "method": request.method,
                "path": "/" + full_path,
                "model": summary.get("model") or model_req,
                "request_model": model_req,
                "status_code": upstream_resp.status_code,
                "latency_ms": latency_ms,
                "is_stream": True,
                "chunk_count": summary.get("chunk_count"),
                "total_stream_bytes": summary.get("total_stream_bytes"),
                "has_error": has_error,
                "error": summary.get("error"),
                "usage": summary.get("usage"),
                "archive_status": summary.get("archive_status"),
                "reconstructed_content_chars": (summary.get("reconstructed") or {}).get("content_text_chars"),
                "reconstructed_content_sha256": (summary.get("reconstructed") or {}).get("content_text_sha256"),
                "record_dir": str(paths["root"]),
                "req_file": str(paths["req"]),
                "res_file": str(paths["res"]),
                "chunks_file": str(paths["chunks"]),
                "headers_file": str(paths["headers"]),
            }
            await enqueue_archive_job(
                write_archive_files,
                [(paths["res"], res_record)],
                family,
                index_record,
                best_effort=True,
            )

    return StreamingResponse(body_iter(), status_code=upstream_resp.status_code, headers=response_headers)

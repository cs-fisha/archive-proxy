#!/usr/bin/env python3
"""
Small benchmark helper for comparing archive-proxy with direct LiteLLM/New API.

Examples:
  python3 tools/benchmark_proxy.py \
    --url http://127.0.0.1:56789/v1/chat/completions \
    --api-key sk-local \
    --model openai/gpt-4o-mini \
    --requests 100 \
    --concurrency 10

  python3 tools/benchmark_proxy.py \
    --url http://127.0.0.1:56789/v1/chat/completions \
    --compare-url http://127.0.0.1:4000/v1/chat/completions \
    --api-key sk-local \
    --model openai/gpt-4o-mini \
    --stream \
    --requests 50 \
    --concurrency 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass
class Result:
    ok: bool
    status_code: Optional[int]
    latency_ms: float
    first_byte_ms: Optional[float]
    bytes_read: int
    error: Optional[str] = None


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    idx = int(round((len(values) - 1) * pct / 100.0))
    return values[max(0, min(idx, len(values) - 1))]


def summarize(name: str, results: List[Result], elapsed_s: float) -> Dict[str, Any]:
    latencies = [r.latency_ms for r in results if r.ok]
    first_bytes = [r.first_byte_ms for r in results if r.ok and r.first_byte_ms is not None]
    total_bytes = sum(r.bytes_read for r in results)
    ok_bytes = sum(r.bytes_read for r in results if r.ok)
    errors = [r for r in results if not r.ok]
    bytes_per_second = total_bytes / elapsed_s if elapsed_s > 0 else None
    ok_bytes_per_second = ok_bytes / elapsed_s if elapsed_s > 0 else None
    return {
        "name": name,
        "requests": len(results),
        "ok": len(results) - len(errors),
        "errors": len(errors),
        "elapsed_s": round(elapsed_s, 3),
        "rps": round(len(results) / elapsed_s, 3) if elapsed_s > 0 else None,
        "ok_rps": round((len(results) - len(errors)) / elapsed_s, 3) if elapsed_s > 0 else None,
        "bytes_read": total_bytes,
        "bytes_per_second": round(bytes_per_second, 2) if bytes_per_second is not None else None,
        "bits_per_second": round(bytes_per_second * 8, 2) if bytes_per_second is not None else None,
        "mbit_per_second": round(bytes_per_second * 8 / 1_000_000, 4) if bytes_per_second is not None else None,
        "ok_bytes_per_second": round(ok_bytes_per_second, 2) if ok_bytes_per_second is not None else None,
        "ok_mbit_per_second": round(ok_bytes_per_second * 8 / 1_000_000, 4) if ok_bytes_per_second is not None else None,
        "latency_ms": {
            "min": round(min(latencies), 2) if latencies else None,
            "p50": round(percentile(latencies, 50) or 0, 2) if latencies else None,
            "p95": round(percentile(latencies, 95) or 0, 2) if latencies else None,
            "p99": round(percentile(latencies, 99) or 0, 2) if latencies else None,
            "max": round(max(latencies), 2) if latencies else None,
            "mean": round(statistics.mean(latencies), 2) if latencies else None,
        },
        "first_byte_ms": {
            "p50": round(percentile(first_bytes, 50) or 0, 2) if first_bytes else None,
            "p95": round(percentile(first_bytes, 95) or 0, 2) if first_bytes else None,
            "p99": round(percentile(first_bytes, 99) or 0, 2) if first_bytes else None,
        },
        "status_counts": dict(Counter(str(r.status_code) for r in results)),
        "sample_errors": [r.error for r in errors[:5]],
    }


def build_payload(args: argparse.Namespace, seq: int) -> Dict[str, Any]:
    prompt = args.prompt or f"Reply with one short sentence. seq={seq}"
    payload: Dict[str, Any] = {
        "model": args.model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": args.max_tokens,
        "stream": args.stream,
    }
    if args.temperature is not None:
        payload["temperature"] = args.temperature
    return payload


async def run_one(client: httpx.AsyncClient, url: str, headers: Dict[str, str], args: argparse.Namespace, seq: int) -> Result:
    payload = build_payload(args, seq)
    started = time.perf_counter()
    first_byte_ms: Optional[float] = None
    bytes_read = 0
    try:
        if args.stream:
            async with client.stream("POST", url, headers=headers, json=payload) as resp:
                async for chunk in resp.aiter_bytes():
                    if chunk:
                        bytes_read += len(chunk)
                        if first_byte_ms is None:
                            first_byte_ms = (time.perf_counter() - started) * 1000
                latency_ms = (time.perf_counter() - started) * 1000
                return Result(200 <= resp.status_code < 400, resp.status_code, latency_ms, first_byte_ms, bytes_read)

        resp = await client.post(url, headers=headers, json=payload)
        bytes_read = len(resp.content)
        latency_ms = (time.perf_counter() - started) * 1000
        return Result(200 <= resp.status_code < 400, resp.status_code, latency_ms, latency_ms, bytes_read)
    except Exception as exc:
        latency_ms = (time.perf_counter() - started) * 1000
        return Result(False, None, latency_ms, first_byte_ms, bytes_read, f"{type(exc).__name__}: {exc}")


async def run_benchmark(name: str, url: str, args: argparse.Namespace) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {args.api_key}",
        "Content-Type": "application/json",
    }
    if args.archive_session_prefix:
        headers["x-archive-session-id"] = f"{args.archive_session_prefix}-{uuid.uuid4().hex[:8]}"

    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    timeout = None if args.timeout <= 0 else args.timeout
    semaphore = asyncio.Semaphore(args.concurrency)
    results: List[Result] = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        async def worker(seq: int) -> None:
            async with semaphore:
                results.append(await run_one(client, url, headers, args, seq))

        started = time.perf_counter()
        await asyncio.gather(*(worker(i) for i in range(args.requests)))
        elapsed_s = time.perf_counter() - started

    return summarize(name, results, elapsed_s)


def compare_summaries(proxy: Dict[str, Any], direct: Dict[str, Any]) -> Dict[str, Any]:
    def ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b in (None, 0):
            return None
        return round(a / b, 4)

    return {
        "latency_p95_ratio": ratio(proxy["latency_ms"]["p95"], direct["latency_ms"]["p95"]),
        "latency_p99_ratio": ratio(proxy["latency_ms"]["p99"], direct["latency_ms"]["p99"]),
        "rps_ratio": ratio(proxy["ok_rps"], direct["ok_rps"]),
        "first_byte_p95_ratio": ratio(proxy["first_byte_ms"]["p95"], direct["first_byte_ms"]["p95"]),
    }


async def main_async(args: argparse.Namespace) -> None:
    output: Dict[str, Any] = {
        "config": {
            "url": args.url,
            "compare_url": args.compare_url,
            "requests": args.requests,
            "concurrency": args.concurrency,
            "stream": args.stream,
            "model": args.model,
            "max_tokens": args.max_tokens,
        },
        "results": [],
    }

    proxy = await run_benchmark("target", args.url, args)
    output["results"].append(proxy)

    if args.compare_url:
        direct = await run_benchmark("compare", args.compare_url, args)
        output["results"].append(direct)
        output["comparison_target_over_compare"] = compare_summaries(proxy, direct)

    print(json.dumps(output, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark archive-proxy or compare it with direct LiteLLM/New API.")
    parser.add_argument("--url", required=True, help="Target chat completions URL, usually archive-proxy.")
    parser.add_argument("--compare-url", default=None, help="Optional direct LiteLLM/New API URL for comparison.")
    parser.add_argument("--api-key", required=True, help="Bearer API key.")
    parser.add_argument("--model", required=True, help="Model name to send in request payload.")
    parser.add_argument("--requests", type=int, default=50, help="Total request count.")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent in-flight requests.")
    parser.add_argument("--max-tokens", type=int, default=64, help="max_tokens in request payload.")
    parser.add_argument("--temperature", type=float, default=0.0, help="Optional temperature.")
    parser.add_argument("--prompt", default=None, help="Prompt text. Defaults to a short generated prompt.")
    parser.add_argument("--stream", action="store_true", help="Use stream=true and measure first-byte latency.")
    parser.add_argument("--timeout", type=float, default=0, help="Total timeout seconds. 0 disables httpx total timeout.")
    parser.add_argument("--archive-session-prefix", default="bench", help="x-archive-session-id prefix. Empty string disables it.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.requests <= 0:
        raise SystemExit("--requests must be > 0")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be > 0")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

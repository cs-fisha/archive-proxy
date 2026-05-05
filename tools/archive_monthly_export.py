#!/usr/bin/env python3
"""
archive_monthly_export.py

Offline monthly exporter for archive-proxy logs.

Features:
  1) Reads archive-proxy archives/index.jsonl, or falls back to walking *-res.json.
  2) Computes input/output/total tokens by model.
  3) Computes New API style cost by model using a pricing JSON file.
  4) Exports reports: summary.json, by_model.csv, by_model.json, missing_pricing.csv.
  5) Merges archive records into JSONL parts <= max size.
  6) Packs raw archive files into ZIP parts <= max size.

Example:
  python archive_monthly_export.py \
    --archive-root /opt/llm-gateway/archives \
    --pricing ./newapi_pricing.json \
    --month 2026-04 \
    --out-dir /opt/llm-gateway/monthly_exports/2026-04 \
    --mode all
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

ARCHIVE_TS_RE = re.compile(r"^(\d{8}T\d{6}\.\d+Z)")
REQ_SUFFIX = "-req.json"
RES_SUFFIX = "-res.json"
HEADERS_SUFFIX = "-headers.json"


@dataclass
class ArchiveRecord:
    ts: str
    request_id: str
    family: Optional[str]
    session_id: Optional[str]
    path: Optional[str]
    request_model: Optional[str]
    model: Optional[str]
    status_code: Optional[int]
    is_stream: Optional[bool]
    has_error: Optional[bool]
    usage: Dict[str, Any]
    req_file: Optional[Path]
    res_file: Optional[Path]
    headers_file: Optional[Path]
    index_record: Optional[Dict[str, Any]] = None


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def parse_archive_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    # archive-proxy timestamp, e.g. 20260430T123001.123456Z
    m = ARCHIVE_TS_RE.match(str(value))
    if m:
        v = m.group(1)
        try:
            return datetime.strptime(v, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    # ISO fallback
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_date_arg(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        return parse_archive_ts(value) or datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception as exc:
        raise SystemExit(f"Invalid date/time: {value!r}: {exc}")


def month_bounds(month: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    if not month:
        return None, None
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise SystemExit("--month must be YYYY-MM, e.g. 2026-04")
    y, m = map(int, month.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(y, m + 1, 1, tzinfo=timezone.utc)
    return start, end


def in_range(ts: str, since: Optional[datetime], until: Optional[datetime]) -> bool:
    dt = parse_archive_ts(ts)
    if dt is None:
        return True
    if since and dt < since:
        return False
    if until and dt >= until:
        return False
    return True


def load_json(path: Optional[Path]) -> Optional[Any]:
    if not path or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        eprint(f"WARN: failed to read JSON {path}: {exc}")
        return None


def json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def resolve_archive_path(path_value: Optional[str], archive_root: Path) -> Optional[Path]:
    if not path_value:
        return None
    p = Path(path_value)
    if p.exists():
        return p
    # index.jsonl may contain container paths like /data/archive/openai/...
    parts = p.parts
    for marker in ("openai", "anthropic"):
        if marker in parts:
            idx = parts.index(marker)
            candidate = archive_root.joinpath(*parts[idx:])
            if candidate.exists():
                return candidate
            return candidate
    return p


def first_nonempty(*values: Any) -> Optional[Any]:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def coerce_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def normalize_model_name(model: Optional[str]) -> Optional[str]:
    if not model:
        return None
    model = str(model).strip()
    if not model:
        return None
    return model


def strip_provider_prefix(model: Optional[str]) -> Optional[str]:
    if not model:
        return None
    s = str(model)
    if "/" in s:
        return s.split("/", 1)[1]
    return s


def get_summary_from_res(res_obj: Any) -> Dict[str, Any]:
    if not isinstance(res_obj, dict):
        return {}
    summary = res_obj.get("summary")
    if isinstance(summary, dict):
        return summary
    return {}


def extract_usage_from_res(res_obj: Any) -> Dict[str, Any]:
    summary = get_summary_from_res(res_obj)
    usage = summary.get("usage")
    if isinstance(usage, dict):
        return usage
    if isinstance(res_obj, dict):
        body = res_obj.get("body")
        if isinstance(body, dict) and body.get("encoding") == "json" and isinstance(body.get("data"), dict):
            data = body["data"]
            if isinstance(data.get("usage"), dict):
                return data["usage"]
    return {}


def extract_request_model(req_obj: Any) -> Optional[str]:
    if not isinstance(req_obj, dict):
        return None
    meta = req_obj.get("meta")
    if isinstance(meta, dict):
        m = meta.get("model")
        if m:
            return str(m)
    body = req_obj.get("body")
    if isinstance(body, dict) and body.get("encoding") == "json" and isinstance(body.get("data"), dict):
        m = body["data"].get("model")
        if m:
            return str(m)
    return None


def normalized_token_usage(usage: Dict[str, Any], include_cache_in_input: bool = False) -> Dict[str, int]:
    """Normalize OpenAI and Anthropic usage shapes.

    OpenAI often returns:
      prompt_tokens, completion_tokens, total_tokens

    Anthropic often returns:
      input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens

    By default cache_* tokens are kept as separate columns and NOT folded into input_tokens,
    because upstream billing policies vary. Use --include-cache-in-input if your New API setup
    bills cache tokens through the same prompt-token formula.
    """
    if not isinstance(usage, dict):
        usage = {}

    prompt_tokens = coerce_int(first_nonempty(usage.get("prompt_tokens"), usage.get("input_tokens")))
    completion_tokens = coerce_int(first_nonempty(usage.get("completion_tokens"), usage.get("output_tokens")))

    cache_creation = coerce_int(first_nonempty(
        usage.get("cache_creation_input_tokens"),
        usage.get("cache_creation_tokens"),
        ((usage.get("prompt_tokens_details") or {}) if isinstance(usage.get("prompt_tokens_details"), dict) else {}).get("cache_creation_tokens"),
    ))
    cache_read = coerce_int(first_nonempty(
        usage.get("cache_read_input_tokens"),
        usage.get("cached_tokens"),
        ((usage.get("prompt_tokens_details") or {}) if isinstance(usage.get("prompt_tokens_details"), dict) else {}).get("cached_tokens"),
    ))

    billing_input_tokens = prompt_tokens + (cache_creation + cache_read if include_cache_in_input else 0)
    total_tokens = coerce_int(usage.get("total_tokens"))
    if total_tokens <= 0:
        total_tokens = prompt_tokens + completion_tokens

    return {
        "input_tokens": prompt_tokens,
        "output_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cache_creation_input_tokens": cache_creation,
        "cache_read_input_tokens": cache_read,
        "billing_input_tokens": billing_input_tokens,
        "billing_output_tokens": completion_tokens,
    }


def iter_index_records(archive_root: Path, since: Optional[datetime], until: Optional[datetime]) -> Iterator[ArchiveRecord]:
    index_path = archive_root / "index.jsonl"
    if not index_path.exists():
        return
    seen: set[Tuple[str, str]] = set()
    with index_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as exc:
                eprint(f"WARN: bad index line {line_no}: {exc}")
                continue
            ts = str(obj.get("ts") or "")
            if not in_range(ts, since, until):
                continue
            request_id = str(obj.get("request_id") or "")
            key = (ts, request_id)
            if key in seen:
                continue
            seen.add(key)
            req_file = resolve_archive_path(obj.get("req_file"), archive_root)
            res_file = resolve_archive_path(obj.get("res_file"), archive_root)
            headers_file = resolve_archive_path(obj.get("headers_file"), archive_root)
            usage = obj.get("usage") if isinstance(obj.get("usage"), dict) else {}
            # Some old index records may miss usage/model; fill from res file lazily.
            if (not usage or not obj.get("model")) and res_file and res_file.exists():
                res_obj = load_json(res_file)
                summary = get_summary_from_res(res_obj)
                if not usage:
                    usage = extract_usage_from_res(res_obj)
                if not obj.get("model"):
                    obj["model"] = summary.get("model")
                if obj.get("status_code") is None:
                    obj["status_code"] = summary.get("status_code")
                if obj.get("has_error") is None:
                    obj["has_error"] = summary.get("has_error")
            if not obj.get("request_model") and req_file and req_file.exists():
                req_obj = load_json(req_file)
                obj["request_model"] = extract_request_model(req_obj)
            yield ArchiveRecord(
                ts=ts,
                request_id=request_id,
                family=obj.get("family"),
                session_id=obj.get("session_id"),
                path=obj.get("path"),
                request_model=normalize_model_name(obj.get("request_model")),
                model=normalize_model_name(obj.get("model") or obj.get("request_model")),
                status_code=coerce_int(obj.get("status_code")) if obj.get("status_code") is not None else None,
                is_stream=obj.get("is_stream"),
                has_error=obj.get("has_error"),
                usage=usage,
                req_file=req_file,
                res_file=res_file,
                headers_file=headers_file,
                index_record=obj,
            )


def derive_sibling_paths(res_file: Path) -> Tuple[Optional[Path], Path, Optional[Path]]:
    name = res_file.name
    if name.endswith(RES_SUFFIX):
        prefix = name[: -len(RES_SUFFIX)]
        return res_file.with_name(prefix + REQ_SUFFIX), res_file, res_file.with_name(prefix + HEADERS_SUFFIX)
    return None, res_file, None


def iter_walk_records(archive_root: Path, since: Optional[datetime], until: Optional[datetime]) -> Iterator[ArchiveRecord]:
    for res_file in archive_root.rglob(f"*{RES_SUFFIX}"):
        m = ARCHIVE_TS_RE.match(res_file.name)
        ts = m.group(1) if m else ""
        if ts and not in_range(ts, since, until):
            continue
        req_file, _, headers_file = derive_sibling_paths(res_file)
        res_obj = load_json(res_file)
        req_obj = load_json(req_file) if req_file and req_file.exists() else None
        summary = get_summary_from_res(res_obj)
        meta = res_obj.get("meta") if isinstance(res_obj, dict) and isinstance(res_obj.get("meta"), dict) else {}
        request_model = extract_request_model(req_obj)
        usage = extract_usage_from_res(res_obj)
        family = meta.get("family")
        if not family:
            parts = res_file.parts
            if "openai" in parts:
                family = "openai"
            elif "anthropic" in parts:
                family = "anthropic"
        rid = str(meta.get("request_id") or res_file.stem.replace("-res", "").split("_")[-1])
        yield ArchiveRecord(
            ts=str(meta.get("ts") or ts),
            request_id=rid,
            family=family,
            session_id=meta.get("session_id"),
            path=None,
            request_model=request_model,
            model=normalize_model_name(summary.get("model") or request_model),
            status_code=coerce_int(summary.get("status_code") or meta.get("status_code")) if (summary.get("status_code") or meta.get("status_code")) is not None else None,
            is_stream=summary.get("is_stream") if "is_stream" in summary else meta.get("is_stream"),
            has_error=summary.get("has_error"),
            usage=usage,
            req_file=req_file if req_file and req_file.exists() else None,
            res_file=res_file,
            headers_file=headers_file if headers_file and headers_file.exists() else None,
            index_record=None,
        )


def load_pricing(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {"pricing_type": "newapi_ratio", "models": {}}
    obj = load_json(path)
    if not isinstance(obj, dict):
        raise SystemExit(f"Pricing file is not a JSON object: {path}")
    obj.setdefault("models", {})
    obj.setdefault("pricing_type", "newapi_ratio")
    return obj


def pricing_candidates(model: Optional[str], request_model: Optional[str], aliases: Dict[str, str]) -> List[str]:
    vals: List[str] = []
    for m in (model, request_model, strip_provider_prefix(model), strip_provider_prefix(request_model)):
        if m and m not in vals:
            vals.append(m)
        if m and aliases.get(m) and aliases[m] not in vals:
            vals.append(aliases[m])
    return vals


def lookup_pricing(pricing: Dict[str, Any], model: Optional[str], request_model: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    models = pricing.get("models") if isinstance(pricing.get("models"), dict) else {}
    aliases = pricing.get("aliases") if isinstance(pricing.get("aliases"), dict) else {}
    for cand in pricing_candidates(model, request_model, aliases):
        if cand in models and isinstance(models[cand], dict):
            return cand, models[cand]
    return None, None


def compute_cost(tokens: Dict[str, int], pricing: Dict[str, Any], price_entry: Optional[Dict[str, Any]]) -> Optional[float]:
    if not price_entry:
        return None
    pricing_type = str(price_entry.get("pricing_type") or pricing.get("pricing_type") or "newapi_ratio")

    in_tok = tokens["billing_input_tokens"]
    out_tok = tokens["billing_output_tokens"]

    if pricing_type in {"per_million", "usd_per_million", "direct"}:
        inp = float(price_entry.get("input_per_million", price_entry.get("prompt_per_million", 0)) or 0)
        out = float(price_entry.get("output_per_million", price_entry.get("completion_per_million", 0)) or 0)
        return (in_tok / 1_000_000.0) * inp + (out_tok / 1_000_000.0) * out

    # New API style: Group ratio × Model ratio × (prompt + completion × completion_ratio) / 500000
    group_ratio = float(price_entry.get("group_ratio", pricing.get("default_group_ratio", pricing.get("group_ratio", 1))) or 1)
    model_ratio = float(price_entry.get("model_ratio", price_entry.get("ratio", 0)) or 0)
    completion_ratio = float(price_entry.get("completion_ratio", pricing.get("default_completion_ratio", 1)) or 1)
    return group_ratio * model_ratio * (in_tok + out_tok * completion_ratio) / 500_000.0


def make_full_jsonl_obj(rec: ArchiveRecord, compact_files: bool = False) -> Dict[str, Any]:
    obj: Dict[str, Any] = {
        "ts": rec.ts,
        "request_id": rec.request_id,
        "family": rec.family,
        "session_id": rec.session_id,
        "path": rec.path,
        "model": rec.model,
        "request_model": rec.request_model,
        "status_code": rec.status_code,
        "is_stream": rec.is_stream,
        "has_error": rec.has_error,
        "usage": rec.usage,
        "files": {
            "req_file": str(rec.req_file) if rec.req_file else None,
            "res_file": str(rec.res_file) if rec.res_file else None,
            "headers_file": str(rec.headers_file) if rec.headers_file else None,
        },
    }
    if not compact_files:
        obj["request"] = load_json(rec.req_file)
        obj["response"] = load_json(rec.res_file)
        obj["headers"] = load_json(rec.headers_file)
    return obj


def write_jsonl_parts(records: List[ArchiveRecord], out_dir: Path, max_bytes: int, mode: str) -> List[Path]:
    out_paths: List[Path] = []
    part_no = 1
    current_size = 0
    current_count = 0
    f = None

    def open_part(n: int):
        path = out_dir / f"archive_merged_part{n:03d}.jsonl"
        return path, path.open("w", encoding="utf-8")

    try:
        path, f = open_part(part_no)
        out_paths.append(path)
        for rec in records:
            if mode == "summary":
                obj = rec.index_record or make_full_jsonl_obj(rec, compact_files=True)
            else:
                obj = make_full_jsonl_obj(rec, compact_files=False)
            line = json_dumps_compact(obj) + "\n"
            b = line.encode("utf-8")
            if current_count > 0 and current_size + len(b) > max_bytes:
                f.close()
                part_no += 1
                path, f = open_part(part_no)
                out_paths.append(path)
                current_size = 0
                current_count = 0
            if len(b) > max_bytes:
                eprint(f"WARN: one JSONL record exceeds max part size: request_id={rec.request_id}, bytes={len(b)}")
            f.write(line)
            current_size += len(b)
            current_count += 1
    finally:
        if f:
            f.close()
    return out_paths


def unique_existing_files(records: List[ArchiveRecord]) -> List[Path]:
    seen: set[Path] = set()
    files: List[Path] = []
    for rec in records:
        for p in (rec.req_file, rec.res_file, rec.headers_file):
            if p and p.exists():
                rp = p.resolve()
                if rp not in seen:
                    seen.add(rp)
                    files.append(p)
    return sorted(files)


def zip_parts(files: List[Path], archive_root: Path, out_dir: Path, max_bytes: int, compression: str) -> List[Path]:
    comp = zipfile.ZIP_DEFLATED if compression == "deflated" else zipfile.ZIP_STORED
    out_paths: List[Path] = []
    part_no = 1
    zf: Optional[zipfile.ZipFile] = None
    zpath: Optional[Path] = None

    def start_zip(n: int) -> Tuple[Path, zipfile.ZipFile]:
        zp = out_dir / f"archive_raw_part{n:03d}.zip"
        return zp, zipfile.ZipFile(zp, "w", compression=comp, allowZip64=True)

    try:
        zpath, zf = start_zip(part_no)
        out_paths.append(zpath)
        for p in files:
            try:
                size = p.stat().st_size
            except OSError:
                continue
            # ZIP overhead estimate. With ZIP_STORED this is conservative enough for 2GB parts.
            overhead = 512 + len(str(p))
            current = zpath.stat().st_size if zpath and zpath.exists() else 0
            if current > 0 and current + size + overhead > max_bytes:
                zf.close()
                part_no += 1
                zpath, zf = start_zip(part_no)
                out_paths.append(zpath)
            if size + overhead > max_bytes:
                eprint(f"WARN: one raw file exceeds max part size: {p} bytes={size}")
            try:
                arcname = str(p.relative_to(archive_root))
            except ValueError:
                arcname = p.name
            zf.write(p, arcname=arcname)
    finally:
        if zf:
            zf.close()
    # Final check. If deflated, exact sizes are only known after close.
    for zp in out_paths:
        if zp.exists() and zp.stat().st_size > max_bytes:
            eprint(f"WARN: ZIP part exceeds max_bytes after close: {zp} size={zp.stat().st_size}")
    return out_paths


def write_reports(records: List[ArchiveRecord], pricing: Dict[str, Any], out_dir: Path, include_cache_in_input: bool) -> Dict[str, Any]:
    by_model: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "requests": 0,
        "errors": 0,
        "stream_requests": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "billing_input_tokens": 0,
        "billing_output_tokens": 0,
        "estimated_cost": 0.0,
        "missing_pricing": False,
        "pricing_key": None,
    })
    grand = {
        "requests": 0,
        "errors": 0,
        "stream_requests": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "billing_input_tokens": 0,
        "billing_output_tokens": 0,
        "estimated_cost": 0.0,
        "missing_pricing_request_count": 0,
    }
    missing: Dict[str, int] = defaultdict(int)

    for rec in records:
        model = normalize_model_name(rec.model or rec.request_model) or "<unknown>"
        usage = normalized_token_usage(rec.usage, include_cache_in_input=include_cache_in_input)
        pricing_key, price_entry = lookup_pricing(pricing, rec.model, rec.request_model)
        cost = compute_cost(usage, pricing, price_entry)

        row = by_model[model]
        row["requests"] += 1
        row["errors"] += 1 if rec.has_error or (rec.status_code is not None and rec.status_code >= 400) else 0
        row["stream_requests"] += 1 if rec.is_stream else 0
        for k in ["input_tokens", "output_tokens", "total_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "billing_input_tokens", "billing_output_tokens"]:
            row[k] += usage[k]
            grand[k] += usage[k]
        if cost is None:
            row["missing_pricing"] = True
            missing[model] += 1
            grand["missing_pricing_request_count"] += 1
        else:
            row["estimated_cost"] += cost
            grand["estimated_cost"] += cost
            if not row["pricing_key"]:
                row["pricing_key"] = pricing_key
        grand["requests"] += 1
        grand["errors"] += 1 if rec.has_error or (rec.status_code is not None and rec.status_code >= 400) else 0
        grand["stream_requests"] += 1 if rec.is_stream else 0

    # Write by_model CSV.
    csv_path = out_dir / "by_model.csv"
    fields = [
        "model", "pricing_key", "requests", "errors", "stream_requests",
        "input_tokens", "output_tokens", "total_tokens",
        "cache_creation_input_tokens", "cache_read_input_tokens",
        "billing_input_tokens", "billing_output_tokens",
        "estimated_cost", "missing_pricing",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for model, row in sorted(by_model.items()):
            writer.writerow({"model": model, **row})

    by_model_json = {model: row for model, row in sorted(by_model.items())}
    (out_dir / "by_model.json").write_text(json_dumps_compact(by_model_json) + "\n", encoding="utf-8")

    missing_path = out_dir / "missing_pricing.csv"
    with missing_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["model", "request_count"])
        for model, count in sorted(missing.items()):
            writer.writerow([model, count])

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "pricing_type": pricing.get("pricing_type", "newapi_ratio"),
        "currency": pricing.get("currency", "USD"),
        "include_cache_in_input": include_cache_in_input,
        "totals": grand,
        "report_files": {
            "by_model_csv": str(csv_path),
            "by_model_json": str(out_dir / "by_model.json"),
            "missing_pricing_csv": str(missing_path),
        },
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary

def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly report/export tool for archive-proxy archives")
    parser.add_argument("--archive-root", default="/home/cs/litellm/archive-proxy/archives", type=Path, help="Archive root, e.g. /opt/llm-gateway/archives")
    parser.add_argument("--pricing", type=Path, default="./newapi_pricing.json", help="Pricing JSON file. Supports New API ratio format or USD-per-million format.")
    parser.add_argument("--out-dir", default=f"./monthly_exports/{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')}", type=Path, help="Output directory")
    parser.add_argument("--month", default=datetime.now(timezone.utc).strftime("%Y-%m"), help="UTC month filter: YYYY-MM")
    parser.add_argument("--since", help="UTC lower bound, inclusive. Example: 2026-04-01 or archive ts")
    parser.add_argument("--until", help="UTC upper bound, exclusive. Example: 2026-05-01 or archive ts")
    parser.add_argument("--mode", choices=["report", "jsonl", "zip", "all"], default="all")
    parser.add_argument("--jsonl-mode", choices=["full", "summary"], default="full", help="full embeds req/res/headers JSON; summary writes index-like rows only")
    parser.add_argument("--max-part-gb", type=float, default=1.95, help="Max part size in GB. Default 1.95 for a safe <2GB margin.")
    parser.add_argument("--zip-compression", choices=["stored", "deflated"], default="stored", help="stored gives more predictable part sizes; deflated is smaller but less predictable")
    parser.add_argument("--walk", action="store_true", help="Ignore index.jsonl and walk *-res.json instead")
    parser.add_argument("--include-cache-in-input", action="store_true", help="Add cache_creation/cache_read tokens to billable input tokens")
    args = parser.parse_args()

    archive_root = args.archive_root.resolve()
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    m_since, m_until = month_bounds(args.month)
    since = parse_date_arg(args.since) or m_since
    until = parse_date_arg(args.until) or m_until

    if args.walk or not (archive_root / "index.jsonl").exists():
        records = list(iter_walk_records(archive_root, since, until))
    else:
        records = list(iter_index_records(archive_root, since, until))

    records.sort(key=lambda r: (r.ts or "", r.request_id or ""))
    pricing = load_pricing(args.pricing)
    max_bytes = int(args.max_part_gb * 1024 ** 3)

    summary = write_reports(records, pricing, out_dir, args.include_cache_in_input)

    produced: Dict[str, List[str]] = {"jsonl_parts": [], "zip_parts": []}
    if args.mode in {"jsonl", "all"}:
        jsonl_dir = out_dir / "jsonl_parts"
        jsonl_dir.mkdir(parents=True, exist_ok=True)
        parts = write_jsonl_parts(records, jsonl_dir, max_bytes, args.jsonl_mode)
        produced["jsonl_parts"] = [str(p) for p in parts]

    if args.mode in {"zip", "all"}:
        zip_dir = out_dir / "zip_parts"
        zip_dir.mkdir(parents=True, exist_ok=True)
        files = unique_existing_files(records)
        parts = zip_parts(files, archive_root, zip_dir, max_bytes, args.zip_compression)
        produced["zip_parts"] = [str(p) for p in parts]

    manifest = {
        "archive_root": str(archive_root),
        "out_dir": str(out_dir),
        "since_utc": since.isoformat() if since else None,
        "until_utc": until.isoformat() if until else None,
        "mode": args.mode,
        "jsonl_mode": args.jsonl_mode,
        "max_part_bytes": max_bytes,
        "record_count": len(records),
        "summary_file": str(out_dir / "summary.json"),
        "produced": produced,
        "totals": summary["totals"],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

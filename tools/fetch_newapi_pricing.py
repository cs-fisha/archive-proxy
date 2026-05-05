#!/usr/bin/env python3
"""
fetch_newapi_pricing.py

Fetch public pricing data from a New API instance and convert it into the
pricing JSON format consumed by archive_monthly_export.py.

Typical usage:
  python3 fetch_newapi_pricing.py \
    --url https://your-new-api.example.com/pricing \
    --out ./newapi_pricing.json \
    --default-group default

The script tries, in order:
  1) <origin>/api/pricing       preferred, contains model ratios + group ratios
  2) <origin>/api/ratio_config  fallback, contains model_ratio/completion_ratio

If your New API instance exposes more pricing details only when logged in,
pass a user token:
  --bearer sk-... or --auth-token ...
  --new-api-user <user_id>
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from tool_config import load_tool_config


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return default
    # tolerate strings like "$2.0000" or "2.0 / 1M"
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    if not m:
        return default
    try:
        return float(m.group(0))
    except Exception:
        return default


def clean_model_name(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s


def origin_from_url(url: str) -> str:
    """Accept https://host/pricing or https://host/api/pricing and return https://host."""
    parsed = urlparse(url if re.match(r"^https?://", url) else "https://" + url)
    if not parsed.scheme or not parsed.netloc:
        raise SystemExit(f"Invalid URL: {url}")
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def http_get_json(url: str, bearer: Optional[str], new_api_user: Optional[str], timeout: float) -> Tuple[int, Dict[str, str], Any]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": "archive-monthly-export/1.0",
    }
    if bearer:
        # Accept both raw token and already-prefixed header value.
        headers["Authorization"] = bearer if bearer.lower().startswith("bearer ") else f"Bearer {bearer}"
    if new_api_user:
        headers["New-Api-User"] = new_api_user
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
            try:
                data = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"{url} did not return valid JSON. First 200 chars: {text[:200]!r}") from exc
            return int(status), dict(resp.headers.items()), data
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to connect to {url}: {exc}") from exc


def unwrap_success_payload(obj: Any) -> Any:
    """New API often returns {success, data, ...}. Keep top-level extras when needed elsewhere."""
    return obj


def find_model_list(payload: Any) -> List[Dict[str, Any]]:
    """Support several New API response variants."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    # Documented /api/pricing shape: {success: true, data: [ ... ], group_ratio: {...}}
    data = payload.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    # Some builds wrap data as {data: {models: [...]}}
    if isinstance(data, dict):
        for key in ("models", "model_pricing", "pricing", "items", "list"):
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        # Some builds use {model_name: {...}}
        if all(isinstance(v, dict) for v in data.values()):
            out: List[Dict[str, Any]] = []
            for k, v in data.items():
                item = dict(v)
                item.setdefault("model_name", k)
                out.append(item)
            if out:
                return out

    for key in ("models", "model_pricing", "pricing", "items", "list"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def find_group_ratio(payload: Any) -> Dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    candidates: List[Any] = [payload.get("group_ratio")]
    data = payload.get("data")
    if isinstance(data, dict):
        candidates.append(data.get("group_ratio"))
    for cand in candidates:
        if isinstance(cand, dict):
            return {str(k): to_float(v, 1.0) for k, v in cand.items()}
    return {}


def model_name_from_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ("model_name", "model", "name", "id"):
        name = clean_model_name(item.get(key))
        if name:
            return name
    return None


def normalize_from_api_pricing(payload: Dict[str, Any], source_url: str, default_group: str, auto_provider_aliases: bool) -> Dict[str, Any]:
    models_list = find_model_list(payload)
    group_ratio_map = find_group_ratio(payload)
    default_group_ratio = group_ratio_map.get(default_group, 1.0)

    out: Dict[str, Any] = {
        "pricing_type": "newapi_ratio",
        "currency": "USD",
        "source": "newapi_api_pricing",
        "source_url": source_url,
        "fetched_at": utc_now(),
        "default_group": default_group,
        "default_group_ratio": default_group_ratio,
        "default_completion_ratio": 1.0,
        "group_ratio": group_ratio_map,
        "aliases": {},
        "models": {},
        "raw_keys": sorted(list(payload.keys())) if isinstance(payload, dict) else [],
    }

    for item in models_list:
        name = model_name_from_item(item)
        if not name:
            continue

        model_ratio = to_float(item.get("model_ratio", item.get("ratio", 0)), 0.0)
        completion_ratio = to_float(item.get("completion_ratio"), out["default_completion_ratio"])
        model_price = item.get("model_price")
        quota_type = item.get("quota_type")
        enable_group = item.get("enable_group") if isinstance(item.get("enable_group"), list) else None

        # For monthly token billing, choose the requested default group if enabled;
        # otherwise use the global default_group_ratio.
        entry_group_ratio = default_group_ratio
        if enable_group and default_group not in [str(x) for x in enable_group]:
            # No better user-specific group information; retain default global ratio.
            entry_group_ratio = default_group_ratio

        # Derived display prices according to New API ratio formula:
        # cost = group_ratio * model_ratio * (prompt + completion * completion_ratio) / 500000
        # input $/1M = group_ratio * model_ratio * 2
        input_per_million = entry_group_ratio * model_ratio * 2.0
        output_per_million = entry_group_ratio * model_ratio * completion_ratio * 2.0

        out["models"][name] = {
            "pricing_type": "newapi_ratio",
            "model_ratio": model_ratio,
            "completion_ratio": completion_ratio,
            "model_price": to_float(model_price, 0.0) if model_price is not None else None,
            "quota_type": quota_type,
            "group_ratio": entry_group_ratio,
            "enable_group": enable_group,
            "input_per_million": input_per_million,
            "output_per_million": output_per_million,
            "raw": item,
        }

        if auto_provider_aliases:
            out["aliases"][f"openai/{name}"] = name
            out["aliases"][f"anthropic/{name}"] = name
            out["aliases"][f"gemini/{name}"] = name

    return out


def normalize_from_ratio_config(payload: Dict[str, Any], source_url: str, default_group: str, auto_provider_aliases: bool) -> Dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        data = payload if isinstance(payload, dict) else {}
    model_ratio = data.get("model_ratio") if isinstance(data.get("model_ratio"), dict) else {}
    completion_ratio = data.get("completion_ratio") if isinstance(data.get("completion_ratio"), dict) else {}
    model_price = data.get("model_price") if isinstance(data.get("model_price"), dict) else {}

    out: Dict[str, Any] = {
        "pricing_type": "newapi_ratio",
        "currency": "USD",
        "source": "newapi_ratio_config",
        "source_url": source_url,
        "fetched_at": utc_now(),
        "default_group": default_group,
        "default_group_ratio": 1.0,
        "default_completion_ratio": 1.0,
        "group_ratio": {},
        "aliases": {},
        "models": {},
        "raw_keys": sorted(list(payload.keys())) if isinstance(payload, dict) else [],
    }

    names = set(map(str, model_ratio.keys())) | set(map(str, completion_ratio.keys())) | set(map(str, model_price.keys()))
    for name in sorted(names):
        mr = to_float(model_ratio.get(name), 0.0)
        cr = to_float(completion_ratio.get(name), out["default_completion_ratio"])
        mp = model_price.get(name)
        out["models"][name] = {
            "pricing_type": "newapi_ratio",
            "model_ratio": mr,
            "completion_ratio": cr,
            "model_price": to_float(mp, 0.0) if mp is not None else None,
            "quota_type": None,
            "group_ratio": 1.0,
            "enable_group": None,
            "input_per_million": mr * 2.0,
            "output_per_million": mr * cr * 2.0,
            "raw": {},
        }
        if auto_provider_aliases:
            out["aliases"][f"openai/{name}"] = name
            out["aliases"][f"anthropic/{name}"] = name
            out["aliases"][f"gemini/{name}"] = name
    return out


def fetch_and_normalize(
    base_or_url: str,
    bearer: Optional[str],
    new_api_user: Optional[str],
    default_group: str,
    timeout: float,
    auto_provider_aliases: bool,
    prefer_ratio_config: bool,
) -> Dict[str, Any]:
    origin = origin_from_url(base_or_url)
    endpoints = [f"{origin}/api/pricing", f"{origin}/api/ratio_config"]
    if prefer_ratio_config:
        endpoints.reverse()

    errors: List[str] = []
    for url in endpoints:
        try:
            status, headers, payload = http_get_json(url, bearer, new_api_user, timeout)
            if not isinstance(payload, dict):
                raise RuntimeError("response JSON is not an object")
            if payload.get("success") is False:
                raise RuntimeError(f"API returned success=false: {payload.get('message')}")

            if url.endswith("/api/pricing"):
                normalized = normalize_from_api_pricing(payload, url, default_group, auto_provider_aliases)
                if normalized["models"]:
                    return normalized
                raise RuntimeError("/api/pricing returned no usable model list")
            else:
                normalized = normalize_from_ratio_config(payload, url, default_group, auto_provider_aliases)
                if normalized["models"]:
                    return normalized
                raise RuntimeError("/api/ratio_config returned no usable model ratios")
        except Exception as exc:
            errors.append(f"{url}: {exc}")
            continue

    raise SystemExit("Failed to fetch New API pricing. Tried:\n  " + "\n  ".join(errors))

def main() -> None:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=None, help="Local config YAML path. Defaults to ./archive-tools.yaml")
    pre_args, _ = pre.parse_known_args()
    cfg = load_tool_config(pre_args.config, "pricing")

    ap = argparse.ArgumentParser(description="Fetch New API pricing and write archive_monthly_export-compatible pricing JSON.", parents=[pre])
    ap.add_argument("--url", "--base-url", dest="url", default=cfg.get("url", "https://your-new-api.example.com/pricing"), help="New API base URL or /pricing page URL, e.g. https://your-new-api.example.com/pricing")
    ap.add_argument("--out", default=cfg.get("out", "./tools/newapi_pricing.json"), help="Output pricing JSON path")
    ap.add_argument("--bearer", "--auth-token", dest="bearer", default=cfg.get("bearer"), help="Optional New API user token / API token for detailed pricing")
    ap.add_argument("--new-api-user", default=cfg.get("new_api_user"), help="Optional New-Api-User header")
    ap.add_argument("--default-group", default=cfg.get("default_group", "default"), help="New API group used for cost calculation, default: default")
    ap.add_argument("--timeout", type=float, default=float(cfg.get("timeout", 30.0)))
    ap.add_argument("--no-provider-aliases", action="store_true", default=not bool(cfg.get("provider_aliases", True)), help="Do not add openai/<model>, anthropic/<model>, gemini/<model> aliases")
    ap.add_argument("--prefer-ratio-config", action="store_true", default=bool(cfg.get("prefer_ratio_config", False)), help="Try /api/ratio_config before /api/pricing")
    ap.add_argument("--pretty", action="store_true", default=True, help="Pretty-print JSON output")
    args = ap.parse_args()

    pricing = fetch_and_normalize(
        base_or_url=args.url,
        bearer=args.bearer,
        new_api_user=args.new_api_user,
        default_group=args.default_group,
        timeout=args.timeout,
        auto_provider_aliases=not args.no_provider_aliases,
        prefer_ratio_config=args.prefer_ratio_config,
    )

    out_path = args.out
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(pricing, f, ensure_ascii=False, indent=2 if args.pretty else None, separators=None if args.pretty else (",", ":"))
        f.write("\n")

    model_count = len(pricing.get("models", {}))
    eprint(f"Wrote {out_path}")
    eprint(f"Source: {pricing.get('source_url')}")
    eprint(f"Models: {model_count}")
    eprint(f"Default group: {pricing.get('default_group')} ratio={pricing.get('default_group_ratio')}")


if __name__ == "__main__":
    main()

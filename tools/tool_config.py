from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_config_candidates() -> Iterable[Path]:
    env_path = os.environ.get("ARCHIVE_PROXY_TOOLS_CONFIG")
    if env_path:
        yield Path(env_path).expanduser()
    yield Path.cwd() / "archive-tools.yaml"
    yield repo_root() / "archive-tools.yaml"


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value[0:1] in {"'", '"'} and value[-1:] == value[0]:
        return value[1:-1]
    lower = value.lower()
    if lower in {"true", "yes", "on"}:
        return True
    if lower in {"false", "no", "off"}:
        return False
    if lower in {"null", "none", "~"}:
        return None
    try:
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+\.\d+", value):
            return float(value)
    except Exception:
        pass
    return value


def _strip_comment(line: str) -> str:
    in_quote: Optional[str] = None
    out = []
    for ch in line:
        if ch in {"'", '"'}:
            in_quote = None if in_quote == ch else ch if in_quote is None else in_quote
        if ch == "#" and in_quote is None:
            break
        out.append(ch)
    return "".join(out).rstrip()


def parse_simple_yaml(path: Path) -> Dict[str, Any]:
    """Parse the small YAML subset used by archive-tools.example.yaml."""
    root: Dict[str, Any] = {}
    stack: list[tuple[int, Dict[str, Any]]] = [(-1, root)]

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        line = _strip_comment(raw_line)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        item = line.strip()
        if ":" not in item:
            raise ValueError(f"Unsupported config line in {path}: {raw_line!r}")
        key, value = item.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: Dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value)

    return root


def load_tool_config(config_path: Optional[str], section: str) -> Dict[str, Any]:
    candidates = [Path(config_path).expanduser()] if config_path else list(default_config_candidates())
    for path in candidates:
        if path.exists():
            data = parse_simple_yaml(path)
            section_data = data.get(section, {})
            if not isinstance(section_data, dict):
                raise SystemExit(f"Config section {section!r} in {path} must be a mapping")
            section_data = dict(section_data)
            section_data["_config_path"] = str(path)
            return section_data
    return {}

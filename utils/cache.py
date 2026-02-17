from __future__ import annotations

import time
from pathlib import Path
from typing import Mapping, Any

CACHE_TTL_SECONDS = 60 * 60 * 24  # 24 hours

def cache_age_seconds(path: Path) -> float:
    if not path.exists():
        return float("inf")
    return time.time() - path.stat().st_mtime

def is_cache_fresh(path: Path) -> bool:
    return path.exists() and cache_age_seconds(path) < CACHE_TTL_SECONDS

def _safe_token(s: str) -> str:
    return (
        s.replace("/", "_")
        .replace(".", "_")
    )

def generate_cache_path(base_dir: Path, *, prefix: str, parts: Mapping[str, Any], ext: str = "csv") -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    safe_prefix = _safe_token(prefix)
    items = [(k, parts[k]) for k in sorted(parts.keys())]

    def norm(v: Any) -> str:
        if v is None:
            return "none"
        return str(v)

    segments = [f"{_safe_token(k)}-{_safe_token(norm(v))}" for k, v in items]
    name = f"{safe_prefix}__{'__'.join(segments)}" if segments else f"{safe_prefix}"
    return base_dir / f"{name}.{ext}"
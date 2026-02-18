from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List
import pandas as pd

from utils.envs import get_envs

def make_run_id() -> str:
    envs = get_envs(log_envs=True)
    start_date = envs.start_date
    end_date = envs.end_date
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%SZ")
    if start_date and end_date:
        return f"{now}__start-{start_date}_end-{end_date}"
    else:
        return now

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def write_jsonl(path: Path, events: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for event in events:
            fp.write(json.dumps(event, ensure_ascii=False) + "\n")

def log_sync_events(
    run_id: str, 
    log_path: Path, 
    to_insert: list, 
    to_update: list, 
    unchanged: int, 
    df: pd.DataFrame, 
    existing_matched: int
) -> None:
    events: List[dict] = []
    for r in to_insert:
        events.append({
            "ts": utc_now_iso(),
            "run_id": run_id,
            "action": "insert",
            "as_of_date": str(r.as_of_date),
            "asset_id": r.asset_id,
            "account_id": r.account_id,
            "quote_currency": r.quote_currency,
            "old_price": None,
            "new_price": str(r.price),
            "source": r.source,
        })
    for r, old_price in to_update:
        events.append({
            "ts": utc_now_iso(),
            "run_id": run_id,
            "action": "update",
            "as_of_date": str(r.as_of_date),
            "asset_id": r.asset_id,
            "account_id": r.account_id,
            "quote_currency": r.quote_currency,
            "old_price": str(old_price),
            "new_price": str(r.price),
            "source": r.source,
        })

    events.append({
        "ts": utc_now_iso(),
        "run_id": run_id,
        "action": "summary",
        "csv_rows": len(df),
        "existing_matched": existing_matched,
        "inserted": len(to_insert),
        "updated": len(to_update),
        "unchanged": unchanged,
        "date_min": str(df["as_of_date"].min()),
        "date_max": str(df["as_of_date"].max()),
    })

    write_jsonl(log_path, events)
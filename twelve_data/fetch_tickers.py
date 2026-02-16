from __future__ import annotations

import os
import json
import time
import random
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from utils.assets import load_assets
from utils.envs import get_envs
from utils.cache import is_cache_fresh, cache_age_seconds, generate_cache_path

BASE_URL = "https://api.twelvedata.com"

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = REPO_ROOT / "assets.json"
CACHE_DIR = REPO_ROOT / "twelve_data" / "cache"
OUT_PATH = REPO_ROOT / "out" / "twelve_data_time_series_1day.csv"

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

OUTPUT_SIZE = "30" # 30 days
INTERVAL = "1day"
MAX_RETRIES = 4

# ----------------------------
# Helpers: API
# ----------------------------

def make_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"apikey {api_key}",
        "Accept": "application/json",
        "User-Agent": "portfolio-pipeline/1.0"
    }

def request_time_series_with_retries(api_key: str, symbol: str, exchange: Optional[str], start_date: date | None, end_date: date | None) -> Dict[str, Any]:
    url = f"{BASE_URL}/time_series"
    headers = make_headers(api_key)

    params: Dict[str, Any] = {
        "symbol": symbol, 
        "interval": INTERVAL
    }

    if start_date and end_date:
        params["start_date"] = start_date.isoformat()
        params["end_date"] = end_date.isoformat()
    else:
        params["outputsize"] = OUTPUT_SIZE
    
    if exchange:
        params["exchange"] = exchange

    for attempt in range(1, MAX_RETRIES + 1):
        try: 
            print(f"GET {url} symbol={symbol} exchange={exchange or '-'} attempt={attempt}/{MAX_RETRIES}")
            r = requests.get(url, params=params, headers=headers, timeout=30)
            print(f"HTTP {r.status_code}")

            data = r.json()
            if data.get("status") == "error":
                raise RuntimeError(f"Twelve Data error: {data}")    
            return data
        except Exception as e:
            wait = (2 ** (attempt - 1)) + random.uniform(0.0, 1.0)
            print(f"  ! error for <{symbol:<12}> exchange={'-' if not exchange else exchange:<8}: {e}")
            print(f"    -> sleeping {wait:.2f}s before retrying...")
            time.sleep(wait)
    
    return {}

def normalize(payload: Dict[str, Any], symbol: str, exchange: Optional[str]) -> pd.DataFrame:
    meta = payload.get("meta") or {}
    values = payload.get("values") or []
    
    if not isinstance(values, list) or not values:
        return pd.DataFrame()
    
    rows: List[Dict[str, any]] = []
    for v in values:
        dt = v.get("datetime")
        close = v.get("close")
        if not dt or close is None:
            continue
        
        rows.append(
            {
                "date": str(dt),
                "close_usd": close,
                "volume": v.get("volume"),
                "symbol": meta.get("symbol") or symbol,
                "exchange": meta.get("exchange") or (exchange or ""),
                "currency": meta.get("currency") or "",
                "interval": meta.get("interval") or INTERVAL,
            }
        )
    
    df = pd.DataFrame(rows)
    if df.empty: 
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["date"].notna()]

    df["close_usd"] = pd.to_numeric(df["close_usd"], errors="coerce")
    df = df[df["close_usd"].notna()]
    df = df[df["close_usd"] > 0]

    if "volume" in df.columns: 
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    
    return df

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    envs = get_envs(required_envs=["TWELVEDATA_API_KEY"])
    api_key = envs.twelvedata_api_key
    force_refresh = envs.force_refresh
    start_date = envs.start_date
    end_date = envs.end_date

    assets = load_assets(ASSETS_PATH)
    print(f"Loaded {len(assets)} assets from {ASSETS_PATH}")
    if (start_date is None) ^ (end_date is None):
        raise SystemExit("If ussing date bounds, you must set both START_DATE and END_DATE (YYYY-MM-DD)")

    all_rows: List[pd.DataFrame] = []

    for asset in assets:
        symbol = asset.twelvedata_symbol
        exchange = asset.twelvedata_exchange

        if not symbol or not exchange:
            continue

        parts: Dict[str, Any] = { "exchange": exchange, "interval": INTERVAL}
        if start_date and end_date:
            parts["start"] = start_date.isoformat()
            parts["end"] = end_date.isoformat()
        else:
            parts["output_size"] = OUTPUT_SIZE
        
        cache_path = generate_cache_path(CACHE_DIR, prefix=symbol, parts=parts, ext="csv")

        if is_cache_fresh(cache_path) and not force_refresh:
            print(f"Loading cache for {symbol:<12} exchange={exchange or '-':<8} -> {cache_path} (age {cache_age_seconds(cache_path)/60:.1f} min)")
            df = pd.read_csv(cache_path)
            all_rows.append(df)
            continue
        
        print(f"Fetching {symbol} exchange={exchange or '-'}")

        payload = request_time_series_with_retries(api_key=api_key, symbol=symbol, exchange=exchange, start_date=start_date, end_date=end_date)
        if not payload:
            print(f"  WARNING: empty payload for {symbol} exchange={exchange or '-'}, skipping.")
            continue
        
        out = normalize(payload, symbol=symbol, exchange=exchange)
        if out.empty: 
            print(f"  WARNING: no values for {symbol} exchange={exchange or '-'}, skipping")
            continue
        
        out.to_csv(cache_path, index=False)
        all_rows.append(out)
        time.sleep(random.uniform(0.0, 1.0))
    
    if not all_rows:
        raise SystemExit("No data fetched for any symbol.")
    
    final_df = pd.concat(all_rows, ignore_index=True)
    final_df = final_df.sort_values(by=["symbol", "exchange", "date"], ascending=[True, True, True])
    final_df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(final_df)} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()
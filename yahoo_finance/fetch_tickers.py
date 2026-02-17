import os
import json
import time
import random
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import yfinance as yf
import pandas as pd

from utils.assets import Asset, load_assets
from utils.envs import get_envs
from utils.cache import is_cache_fresh, cache_age_seconds, generate_cache_path
from utils.retry import retry_with_backoff

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = REPO_ROOT / "assets.json"
CACHE_DIR = REPO_ROOT / "yahoo_finance" / "cache"
OUT_PATH = REPO_ROOT / "out" / "yahoo_finance_tickers_eod.csv"

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

PERIOD = "1mo"
INTERVAL = "1d"

def _get_cached_data(ticker: str, start_date: date | None, end_date: date | None, force_refresh: bool) -> tuple[Path, pd.DataFrame | None]:
    parts: Dict[str, Any] = {"interval": INTERVAL}
    if start_date and end_date:
        parts["start"] = start_date.isoformat()
        parts["end"] = end_date.isoformat()
    else:
        parts["period"] = PERIOD
    
    cache_path = generate_cache_path(CACHE_DIR, prefix=ticker, parts=parts, ext="csv")
    
    if is_cache_fresh(cache_path) and not force_refresh:
        print(f"Loading cache for {ticker:<10} -> {cache_path} (age {cache_age_seconds(cache_path)/60:.1f} min)")
        return cache_path, pd.read_csv(cache_path)
    
    return cache_path, None

def _download_close_prices(ticker: str, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
    params = dict(interval=INTERVAL, progress=False, threads=False)
    if start_date and end_date:
        params["start"] = start_date.strftime("%Y-%m-%d")
        params["end"] = end_date.strftime("%Y-%m-%d")
    else:
        params["period"] = PERIOD
    
    df = retry_with_backoff(lambda: yf.download(ticker, **params), identifier=ticker)
    if df is None or df.empty:
        return pd.DataFrame()
    return df

def _normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = df.reset_index()
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    out = df[["Date", "Close", "Volume"]].copy()
    out["ticker"] = ticker
    out = out.rename(columns={"Date": "date", "Close": "close_ars", "Volume": "volume"})
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    return out

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    envs = get_envs()
    force_refresh = envs.force_refresh
    start_date = envs.start_date
    end_date = envs.end_date

    # TODO: should we filter right after we load them by the ones which have 'yfinance_symbol'? 
    # TODO: could that filtering by keys presence be done directly in the utils passing a param?
    assets = load_assets(ASSETS_PATH)
    print(f"Loaded {len(assets)} assets from {ASSETS_PATH}")

    all_rows: List[pd.DataFrame] = []

    for asset in assets: 
        ticker = asset.yfinance_symbol
        if not ticker:
            continue

        cache_path, cached_df = _get_cached_data(ticker, start_date, end_date, force_refresh)
        if cached_df is not None:
             all_rows.append(cached_df)
             continue

        print(f"Fetching {ticker}...")
        df = _download_close_prices(ticker, start_date, end_date)
        if df is None or df.empty:
            print(f"  WARNING: no data returned for <{ticker:<10}>, skipping.")
            continue

        out = _normalize(df, ticker)
        if out.empty: 
            print(f"  WARNING: no values for <{ticker:<10}>, skipping")
            continue

        out.to_csv(cache_path, index=False)
        all_rows.append(out)
        time.sleep(random.uniform(0.0, 1.0))  # Sleep to avoid hitting rate limits

    if not all_rows: 
        raise SystemExit("No data fetched for any ticker.")

    final_df = pd.concat(all_rows, ignore_index=True)
    final_df = final_df.sort_values(by=["ticker", "date"], ascending=[True, True])
    final_df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(final_df)} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()
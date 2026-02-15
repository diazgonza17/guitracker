import os
import json
import time
import random
from datetime import date
from pathlib import Path
from typing import Any, Dict, List
from utils.load_assets import Asset, load_assets

import yfinance as yf
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = REPO_ROOT / "assets.json"
CACHE_DIR = REPO_ROOT / "yahoo_finance" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = REPO_ROOT / "out" / "yahoo_finance_tickers_eod.csv"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

CACHE_TTL_SECONDS = 60 * 60 * 24  # 24 hours

PERIOD = "1mo"
INTERVAL = "1d"
MAX_RETRIES = 4

# ----------------------------
# Helpers: env
# ----------------------------

def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    if val.lower() in ("1", "true", "yes", "on") :
        return True
    if val.lower() in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"Invalid boolean value for {name}: {val}")

def env_date(name: str) -> date | None:
    val = os.getenv(name)
    if not val:
        return None
    try:
        return pd.to_datetime(val, format="%Y-%m-%d").date()
    except Exception as e:
        raise ValueError(f"Invalid date value for {name}: {val}") from e


# ----------------------------
# Helpers: cache
# ----------------------------

def cache_age_seconds(path: Path) -> float:
    if not path.exists():
        return float("inf")
    return time.time() - path.stat().st_mtime

def is_cache_fresh(path: Path) -> bool:
    return path.exists() and cache_age_seconds(path) < CACHE_TTL_SECONDS

def generate_cache_key(ticker: str, start_date: date | None, end_date: date | None) -> str:
    safe_ticker = ticker.replace(".", "_")
    # TODO: if we add the XOR safeguard for date bounds, here we wouldnt need the if ... else "none" thing
    if start_date and end_date:
        start = start_date.isoformat() if start_date else "none"
        end = end_date.isoformat() if end_date else "none"
        return f"{safe_ticker}__interval-{INTERVAL}__start-{start}__end-{end}"
    else:
        return f"{safe_ticker}__interval-{INTERVAL}__period-{PERIOD}"

# ----------------------------
# Helpers: API
# ----------------------------

def download_with_retries(ticker: str, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
    # TODO: we could move retrier logic (aside from the fetch itself, the yf.download) to a helper in utils
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # TODO: shouldnt we make the params outside the for loop?
            params = dict(interval=INTERVAL, progress=False, threads=False)
            if start_date and end_date:
                params["start"] = start_date.strftime("%Y-%m-%d")
                params["end"] = end_date.strftime("%Y-%m-%d")
            else:
                params["period"] = PERIOD

            df = yf.download(ticker, **params)
            return df
        except Exception as e:
            wait = (2 ** (attempt - 1)) + random.uniform(0.0, 1.0) # Exponential backoff with jitter
            print(f"  ! error for <{ticker:<10}> (attempt {attempt}/{MAX_RETRIES}): {e}")
            print(f"    -> sleeping {wait:.2f}s before retrying...")
            time.sleep(wait)

    return pd.DataFrame()  # Return empty DataFrame if all retries fail

def normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
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
    # TODO: should we filter right after we load them by the ones which have 'yfinance_symbol'? 
    # TODO: could that filtering by keys presence be done directly in the utils passing a param?
    assets = load_assets(ASSETS_PATH)
    print(f"Loaded {len(assets)} assets from {ASSETS_PATH}")

    # TODO: could we get and print all env vars from a helper in utils?
    force_refresh = env_bool("FORCE_REFRESH")
    start_date = env_date("START_DATE")
    end_date = env_date("END_DATE")
    # TODO: should we add the safeguard when passing only one date bound?
    print(f"FORCE_REFRESH={force_refresh} START_DATE={start_date} END_DATE={end_date}")

    all_rows: List[pd.DataFrame] = []

    for asset in assets: 
        ticker = asset.yfinance_symbol
        if not ticker:
            continue

        # TODO: should we move cache path generation to a helper in utils?
        cache_key = generate_cache_key(ticker, start_date, end_date)
        cache_path = CACHE_DIR / f"{cache_key}.csv"

        if is_cache_fresh(cache_path) and not force_refresh:
            print(f"Loading cache for {ticker:<10} -> {cache_path} (age {cache_age_seconds(cache_path)/60:.1f} min)")
            df = pd.read_csv(cache_path)
            all_rows.append(df)
            continue

        print(f"Fetching {ticker}...")
        df = download_with_retries(ticker, start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            print(f"  WARNING: no data returned for <{ticker:<10}>, skipping.")
            continue

        out = normalize(df, ticker)
        # TODO: should we check for emptyness here?
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
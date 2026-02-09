import os
import json
import time
import random
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

# ----------------------------
# Helpers: cache
# ----------------------------

def cache_age_seconds(path: Path) -> float:
    """
    Returns the age of the cache file in seconds, or infinity if it doesn't exist.
    """
    if not path.exists():
        return float("inf")

    return time.time() - path.stat().st_mtime

def is_cache_fresh(path: Path) -> bool:
    """
    Returns True if the cache file exists and is younger thatn CACHE_TTL_SECONDS.
    """
    return path.exists() and cache_age_seconds(path) < CACHE_TTL_SECONDS

# ----------------------------
# Helpers: fetching + normalization
# ----------------------------

def download_with_retries(ticker: str, retries: int = MAX_RETRIES) -> pd.DataFrame:
    """
    Download history for one ticker, retrying on transient failures.
    """
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker,
                period=PERIOD,
                interval=INTERVAL,
                progress=False,
                threads=False,
            )
            return df
        except Exception as e:
            wait = (2 ** (attempt - 1)) + random.uniform(0.0, 1.0) # Exponential backoff with jitter
            print(f"  ! error for <{ticker:<10}> (attempt {attempt}/{retries}): {e}")
            print(f"    -> sleeping {wait:.2f}s before retrying...")
            time.sleep(wait)

    return pd.DataFrame()  # Return empty DataFrame if all retries fail

def normalize(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Turn yfinance output into a clean table:
    date, ticker, close_ars, volume
    """
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
    assets = load_assets(ASSETS_PATH)
    print(f"Loaded {len(assets)} assets from {ASSETS_PATH}")
    force_refresh = env_bool("FORCE_REFRESH", default=False)
    print(f"FORCE_REFRESH={force_refresh}")

    all_rows: List[pd.DataFrame] = []

    for asset in assets: 
        ticker = asset.yfinance_symbol
        if not ticker:
            continue

        cache_path = CACHE_DIR / f"{ticker.replace('.', '_')}.csv"

        if is_cache_fresh(cache_path) and not force_refresh:
            print(f"Loading cache for {ticker:<10} -> {cache_path} (age {cache_age_seconds(cache_path)/60:.1f} min)")
            df = pd.read_csv(cache_path)
            all_rows.append(df)
            continue

        print(f"Fetching {ticker}...")
        df = download_with_retries(ticker)
        
        if df is None or df.empty:
            print(f"  WARNING: no data returned for <{ticker:<10}>, skipping.")
            continue

        out = normalize(df, ticker)
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
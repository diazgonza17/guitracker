from __future__ import annotations

import sys
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import psycopg

from utils.assets import Asset, load_assets
from utils.envs import get_envs
from utils.prices import PriceRow, fetch_existing_prices, diff_prices, upsert_prices
from utils.log import utc_now_iso, write_jsonl, log_sync_events
from utils.csv import validate_and_load_csv, validate_csv_symbols_against_assets, build_price_rows

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = REPO_ROOT / "assets.json"
CSV_PATH = REPO_ROOT / "out" / "twelvedata_prices.csv"
LOG_DIR = REPO_ROOT / "out"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    envs = get_envs(required_envs=["DATABASE_URL", "RUN_ID"])
    database_url = envs.database_url
    run_id = envs.run_id
    log_path = LOG_DIR / f"twelvedata_sync_{run_id}.jsonl"

    df = validate_and_load_csv(CSV_PATH, symbol_column="twelvedata_symbol")
    symbols = sorted(df["twelvedata_symbol"].dropna().unique().tolist())
    print(f"CSV OK: {len(df)} rows. {len(symbols)} symbols. Date range: {df['as_of_date'].min()} - {df['as_of_date'].max()}")
    
    assets = load_assets(ASSETS_PATH)
    by_symbol = { a.twelvedata_symbol: a for a in assets if a.twelvedata_symbol }
    validate_csv_symbols_against_assets(symbols, by_symbol, symbol_column="twelvedata_symbol")
    print(f"assets.json OK.")
    
    fetched_prices = build_price_rows(df, by_symbol, symbol_column="twelvedata_symbol")
    keys = [(r.as_of_date, r.asset_id, r.account_id) for r in fetched_prices] 
        
    try: 
        with psycopg.connect(database_url) as conn:
            existing_prices = fetch_existing_prices(conn, keys)
            print(f"Found {len(existing_prices)} existing price entries matching CSV data.")

            to_insert, to_update, unchanged = diff_prices(fetched_prices, existing_prices)
            print(f"Plan: insert {len(to_insert)}, update {len(to_update)}, unchanged {unchanged}.")

            print(f"Logging changes to {log_path}")
            log_sync_events(
                run_id=run_id,
                log_path=log_path,
                to_insert=to_insert,
                to_update=to_update,
                unchanged=unchanged,
                df=df,
                existing_matched=len(existing_prices),
            )

            upsert_prices(conn, fetched_prices)
            conn.commit()
            print(f"Upsert complete.")
        
    except Exception as e:
        print(f"  ! error connecting to database: {e}")
        sys.exit(1)
                
if __name__ == "__main__":
    main()
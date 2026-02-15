from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from utils.load_assets import Asset, load_assets

import pandas as pd
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = REPO_ROOT / "assets.json"
CSV_PATH = REPO_ROOT / "out" / "yahoo_finance_tickers_eod.csv"
LOG_DIR = REPO_ROOT / "out"
LOG_DIR.mkdir(parents=True, exist_ok=True)

@dataclass(frozen=True)
class PriceRow: 
    as_of_date: pd.Timestamp
    asset_id: str
    account_id: str
    quote_currency: str
    price: Decimal
    source: str = "yahoo_finance"

Key = Tuple[pd.Timestamp, str, str]

# ----------------------------
# Helpers: CSV validation and loading
# ----------------------------

def validate_and_load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"CSV file not found at {path}, run yahoo_finance/fetch_tickers.py first")

    df = pd.read_csv(path)

    required_columns = {"date", "close_ars", "ticker"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise SystemExit(f"CSV file is missing required columns: {sorted(missing_columns)}. Found columns: {list(df.columns)}")
    
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if df["date"].isna().any():
        bad = df[df["date"].isna()]
        raise SystemExit(f"Found invalid date values in CSV file: {bad}")
    
    df["close_ars"] = pd.to_numeric(df["close_ars"], errors="coerce")
    if df["close_ars"].isna().any():
        bad = df[df["close_ars"].isna()]
        raise SystemExit(f"Found invalid close_ars values in CSV file: {bad}")

    if(df["close_ars"] <= 0).any():
        bad = df[df["close_ars"] <= 0]
        raise SystemExit(f"Found non-positive close_ars values in CSV file: {bad}")
    
    rows = len(df)
    tickers = sorted(df["ticker"].dropna().unique().tolist())
    min_date = df["date"].min()
    max_date = df["date"].max()

    return df

# ----------------------------
# Helpers: assets.json validation and mapping
# ----------------------------

def validate_csv_tickers_against_assets(tickers: List[str], by_ticker: Dict[str, Asset]) -> None:
    missing_tickers = sorted(set(tickers) - set(by_ticker.keys()))
    if missing_tickers:
        raise SystemExit(f"Tickers in CSV not found in assets.json: {', '.join(missing_tickers)}")
    
    wrong_ccy = sorted(
        t for t in tickers
        if by_ticker[t].quote_currency != "ARS"
    )
    if wrong_ccy:
        raise SystemExit(f"Tickers in CSV with non-ARS quote currency: {', '.join(wrong_ccy)}")

def build_price_rows(df: pd.DataFrame, by_ticker: Dict[str, Asset]) -> List[PriceRow]:
    out: List[PriceRow] = []
    for r in df.itertuples(index=False):
        ticker = r.ticker
        asset = by_ticker[ticker]
        out.append(PriceRow(
            as_of_date=r.date,
            asset_id=asset.asset_id,
            account_id=asset.account_id,
            quote_currency=asset.quote_currency,
            price=Decimal(str(r.close_ars)),
        ))
    return out

# ----------------------------
# Helpers: fetch and compare with DB
# ----------------------------

def fetch_existing_prices(conn: psycopg.Connection, keys: Iterable[Key]) -> Dict[Key, Decimal]: 
    key_list = list(keys)
    if not key_list:
        return {}
    
    dates = [k[0] for k in key_list]
    asset_ids = [k[1] for k in key_list]
    account_ids = [k[2] for k in key_list]

    sql = """
        WITH wanted AS (
            SELECT * FROM unnest(%s::date[], %s::text[], %s::text[])
            AS t(as_of_date, asset_id, account_id)
        )
        SELECT p.as_of_date, p.asset_id, p.account_id, p.price
        FROM prices_snapshots p
        JOIN wanted w
            ON p.as_of_date = w.as_of_date
            AND p.asset_id = w.asset_id
            AND p.account_id = w.account_id
    """

    existing: Dict[Key, Decimal] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (dates, asset_ids, account_ids))
        for as_of_date, asset_id, account_id, price in cur.fetchall():
            existing[(as_of_date, asset_id, account_id)] = Decimal(price)
    return existing

def diff_prices(desired: List[PriceRow], existing: Dict[Key, Decimal]):
    to_insert: List[PriceRow] = []
    to_update: List[Tuple[PriceRow, Decimal]] = []
    unchanged: int = 0

    for row in desired:
        key: Key = (row.as_of_date, row.asset_id, row.account_id)
        old = existing.get(key)
        if old is None:
            to_insert.append(row)
            continue
        if Decimal(old) != row.price:
            to_update.append((row, Decimal(old)))
        else:
            unchanged += 1

    return to_insert, to_update, unchanged

def upsert_prices(conn: psycopg.Connection, rows: List[PriceRow]) -> None:
    if not rows:
        print("No rows to upsert.")
        return

    sql = """
        INSERT INTO prices_snapshots (as_of_date, asset_id, account_id, quote_currency, price, source, fetched_at)
        VALUES (%(as_of_date)s, %(asset_id)s, %(account_id)s, %(quote_currency)s, %(price)s, %(source)s, NOW())
        ON CONFLICT (as_of_date, asset_id, account_id) DO UPDATE
        SET price = EXCLUDED.price, quote_currency = EXCLUDED.quote_currency, source = EXCLUDED.source, fetched_at = NOW()
        WHERE prices_snapshots.price IS DISTINCT FROM EXCLUDED.price OR prices_snapshots.quote_currency IS DISTINCT FROM EXCLUDED.quote_currency
    """

    payload = [
        {
            "as_of_date": r.as_of_date,
            "asset_id": r.asset_id,
            "account_id": r.account_id,
            "quote_currency": r.quote_currency,
            "price": r.price,
            "source": r.source,
        }
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.executemany(sql, payload)


# ----------------------------
# Helpers: logging
# ----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def write_jsonl(path: Path, events: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for event in events:
            fp.write(json.dumps(event, ensure_ascii=False) + "\n")

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    df = validate_and_load_csv(CSV_PATH)
    tickers = sorted(df["ticker"].dropna().unique().tolist())
    print(f"CSV OK: {len(df)} rows. {len(tickers)} tickers. date range: {df['date'].min()} -> {df['date'].max()}.")

    assets = load_assets(ASSETS_PATH)
    by_ticker = { a.yfinance_symbol: a for a in assets if a.yfinance_symbol }
    validate_csv_tickers_against_assets(tickers, by_ticker)
    print(f"assets.json OK.")

    desired = build_price_rows(df, by_ticker)
    keys = [(r.as_of_date, r.asset_id, r.account_id) for r in desired] 

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("Missing DATABASE_URL environment variable")

    try: 
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                existing = fetch_existing_prices(conn, keys)
                print(f"Found {len(existing)} existing price entries matching CSV data.")

                to_insert, to_update, unchanged = diff_prices(desired, existing)
                print(f"Plan: insert {len(to_insert)}, update {len(to_update)}, unchanged {unchanged}.")

                run_id = os.getenv("RUN_ID")
                log_path = LOG_DIR / f"yahoo_finance_sync_{run_id}.jsonl"
                print(f"Logging changes to {log_path}.")

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
                    "existing_matched": len(existing),
                    "inserted": len(to_insert),
                    "updated": len(to_update),
                    "unchanged": unchanged,
                    "date_min": str(df["date"].min()),
                    "date_max": str(df["date"].max()),
                })

                write_jsonl(log_path, events)

                upsert_prices(conn, desired)
                conn.commit()
                print(f"Upsert complete.")

    except Exception as e:
        print(f"  ! error connecting to database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
                
from __future__ import annotations

from datetime import date
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple

import psycopg
import pandas as pd

@dataclass(frozen=True)
class PriceRow: 
    as_of_date: date
    asset_id: str
    account_id: str
    quote_currency: str
    price: Decimal
    source: str

Key = Tuple[date, str, str]

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

    existing_prices: Dict[Key, Decimal] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (dates, asset_ids, account_ids))
        for as_of_date, asset_id, account_id, price in cur.fetchall():
            existing_prices[(as_of_date, asset_id, account_id)] = Decimal(price)
    return existing_prices

def diff_prices(fetched_prices: List[PriceRow], existing_prices: Dict[Key, Decimal]):
    to_insert: List[PriceRow] = []
    to_update: List[Tuple[PriceRow, Decimal]] = []
    unchanged: int = 0

    for row in fetched_prices:
        key: Key = (row.as_of_date, row.asset_id, row.account_id)
        old = existing_prices.get(key)
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
from __future__ import annotations

from pathlib import Path
from typing import Dict, List
from decimal import Decimal

import pandas as pd

from utils.assets import Asset
from utils.prices import PriceRow

def validate_and_load_csv(path: Path, *, symbol_column: str) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"CSV file not found at {path}, run {path.parent.name}/fetch.py first")

    df = pd.read_csv(path)

    required_columns = {"as_of_date", "price", symbol_column}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise SystemExit(f"CSV file is missing required columns: {sorted(missing_columns)}. Found columns: {list(df.columns)}")
    
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    if df["as_of_date"].isna().any():
        bad = df[df["as_of_date"].isna()]
        raise SystemExit(f"Found invalid as_of_date values in CSV file: {bad}")
    
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if df["price"].isna().any():
        bad = df[df["price"].isna()]
        raise SystemExit(f"Found invalid price values in CSV file: {bad}")
    
    if (df["price"] <= 0).any():
        bad = df[df["price"] <= 0]
        raise SystemExit(f"Found non-positive price values in CSV file: {bad}")
    
    df[symbol_column] = df[symbol_column].astype(str)
    if (df[symbol_column].str.strip() == "").any():
        bad = df[df[symbol_column].str.strip() == ""]
        raise SystemExit(f"Found invalid {symbol_column} values in CSV file: {bad}")

    df = df.sort_values([symbol_column, "as_of_date"]).drop_duplicates(subset=[symbol_column, "as_of_date"], keep="last")
    return df

def validate_csv_symbols_against_assets(symbols: List[str], by_symbol: Dict[str, Asset], symbol_column: str) -> None:
    missing = sorted(set(symbols) - set(by_symbol.keys()))
    if missing:
        raise SystemExit(f"{symbol_column} values in CSV not found in assets.json: {', '.join(missing)}")

_SOURCE_MAP = {
    "yfinance_symbol": "yahoo_finance",
    "twelvedata_symbol": "twelvedata",
}

def build_price_rows(df: pd.DataFrame, by_symbol: Dict[str, Asset], symbol_column: str) -> List[PriceRow]:
    out: List[PriceRow] = []

    for r in df.itertuples(index=False):
        asset = by_symbol[getattr(r, symbol_column)]
        out.append(PriceRow(
            as_of_date=r.as_of_date,
            asset_id=asset.asset_id,
            account_id=asset.account_id,
            quote_currency=asset.quote_currency,
            price=Decimal(str(r.price)),
            source=_SOURCE_MAP.get(symbol_column, ""),
        ))
    return out
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

@dataclass(frozen=True)
class Asset:
    asset_id: str
    account_id: str
    quote_currency: str

    yfinance_symbol: Optional[str] = None

    twelvedata_symbol: Optional[str] = None
    twelvedata_exchange: Optional[str] = None

def load_assets(path: Path) -> List[Asset]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Expected list in {path}, got {type(raw)}")
    
    out: List[Asset] = []
    for a in raw:
        if not isinstance(a, dict):
            continue
        
        for key in ("asset_id", "account_id", "quote_currency"):
            if key not in a:
                raise ValueError(f"Asset entry {a} is missing required key '{key}'")
        
        quote_currency = a["quote_currency"]
        if quote_currency not in ("ARS", "USD"):
            raise ValueError(f"Unsupported quote_currency '{quote_currency}' in asset {a}")
        
        asset = Asset(
            asset_id=a["asset_id"],
            account_id=a["account_id"],
            quote_currency=quote_currency,
            yfinance_symbol=a.get("yfinance_symbol"),
            twelvedata_symbol=a.get("twelvedata_symbol"),
            twelvedata_exchange=a.get("twelvedata_exchange"),
        )
        out.append(asset)
    return out


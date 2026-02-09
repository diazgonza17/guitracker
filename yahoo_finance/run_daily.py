from __future__ import annotations

import sys

from yahoo_finance.fetch_tickers import main as fetch_tickers
from yahoo_finance.sync_db import main as sync_db

def main() -> None:
    print("== Step 1/2: fetch_tickers ==")
    fetch_tickers()

    print("== Step 2/2: sync_db ==")
    sync_db()

    print("== Done ==")

if __name__ == "__main__":
    try: 
        main()
    except Exception as e:
        print(f"  ! error during execution: {e}")
        raise
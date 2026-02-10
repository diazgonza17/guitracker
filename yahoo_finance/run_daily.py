from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from yahoo_finance.fetch_tickers import main as fetch_tickers
from yahoo_finance.sync_db import main as sync_db

def make_run_id() -> str:
    start = os.getenv("START_DATE") or None
    end = os.getenv("END_DATE") or None
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    if start and end:
        return f"{now}__start-{start}_end-{end}"
    else:
        return now

def main() -> None:
    run_id = make_run_id()
    os.environ["RUN_ID"] = run_id
    print(f"RUN_ID={run_id}")

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
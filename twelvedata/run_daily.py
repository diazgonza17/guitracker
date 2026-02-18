from __future__ import annotations

import os
import sys

from utils.log import make_run_id
from twelvedata.fetch import main as fetch
from twelvedata.sync import main as sync

def main() -> None:
    run_id = make_run_id()
    os.environ["RUN_ID"] = run_id
    
    print("== Step 1/2: fetch ==")
    fetch()

    print("== Step 2/2: sync ==")
    sync()

    print("== Done ==")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"  ! error during execution: {e}")
        raise
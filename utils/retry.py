from __future__ import annotations

import random
import time
from typing import Callable, Any, TypeVar

MAX_RETRIES = 4

T = TypeVar("T")


def retry_with_backoff(fn: Callable[[], T], *, identifier: str, jitter: bool = True) -> T | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            wait = 2 ** (attempt - 1) # Exponential backoff
            if jitter:
                wait += random.uniform(0.0, 1.0) # Add jitter

            print(f"  ! error for <{identifier:<10}> (attempt {attempt}/{MAX_RETRIES}): {e}")
            print(f"    -> sleeping {wait:.2f}s before retrying...")
            time.sleep(wait)
    return None
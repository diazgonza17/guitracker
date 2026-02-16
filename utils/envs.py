import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, Optional

import pandas as pd

_ENV_SPECS: Dict[str, Dict[str, Any]] = {
    "FORCE_REFRESH": {"type": "bool", "default": False},
    "START_DATE": {"type": "date", "default": None},
    "END_DATE": {"type": "date", "default": None},
    "TWELVEDATA_API_KEY": {"type": "str", "default": None, "secret": True},
}

def _parse_bool(val: str) -> bool:
    v = val.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"Invalid boolean value: {val}")

def _parse_date(val: str) -> date:
    return pd.to_datetime(val, format="%Y-%m-%d").date()

def _validate_date_bounds(data: Dict[str, Any]) -> None:
    start_date = data.get("START_DATE")
    end_date = data.get("END_DATE")

    if (start_date is None) ^ (end_date is None):
        raise ValueError("START_DATE and END_DATE must be set together")
    if start_date and end_date:
        if start_date > end_date:
            raise ValueError("START_DATE must be before END_DATE")

def _log_public_envs(data: Dict[str, Any]) -> None:
    for env_name, spec in _ENV_SPECS.items():
        if spec.get("secret", False):
            continue
        print(f"{env_name}={data[env_name]}")    

@dataclass(frozen=True)
class Envs:
    force_refresh: bool
    start_date: Optional[date]
    end_date: Optional[date]
    twelvedata_api_key: Optional[str]

def get_envs(*, required_envs: Iterable[str] = ()) -> Envs:
    required_set = set(required_envs)
    values_by_env_name: Dict[str, Any] = {}
    for env_name, spec in _ENV_SPECS.items():
        raw = os.getenv(env_name)

        if raw is None or raw.strip() == "":
            if env_name in required_set:
                raise ValueError(f"Missing required environment variable: {env_name}")
            values_by_env_name[env_name] = spec.get("default")
            continue
        
        t = spec.get("type")
        try:
            if t == "bool":
                values_by_env_name[env_name] = _parse_bool(raw)
            elif t == "date":
                values_by_env_name[env_name] = _parse_date(raw)
            elif t == "str":
                values_by_env_name[env_name] = raw
            else:
                raise ValueError(f"Invalid spec type for {env_name}: {t}")
        except Exception as e:
            raise ValueError(f"Invalid value for {env_name}: {raw} (expected {t})") from e

    _validate_date_bounds(values_by_env_name)
    _log_public_envs(values_by_env_name)

    return Envs(
        force_refresh=values_by_env_name["FORCE_REFRESH"],
        start_date=values_by_env_name["START_DATE"],
        end_date=values_by_env_name["END_DATE"],
        twelvedata_api_key=values_by_env_name["TWELVEDATA_API_KEY"],
    )
    
    
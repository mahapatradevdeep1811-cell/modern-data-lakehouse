"""
config_loader.py
~~~~~~~~~~~~~~~~
Loads and merges YAML configuration files with environment variable substitution.
Exposes a single `get_config()` function used across the entire pipeline.
"""

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

load_dotenv()

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::-(.*?))?\}")


def _substitute_env_vars(value: str) -> str:
    """Replace ${VAR:-default} placeholders with environment variable values."""
    def replacer(match):
        var_name, default = match.group(1), match.group(2) or ""
        return os.environ.get(var_name, default)
    return _ENV_VAR_PATTERN.sub(replacer, value)


def _process_values(obj: Any) -> Any:
    """Recursively substitute env vars in all string values."""
    if isinstance(obj, dict):
        return {k: _process_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_process_values(i) for i in obj]
    if isinstance(obj, str):
        return _substitute_env_vars(obj)
    return obj


def _load_yaml(path: Path) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Deep merge two dicts; override wins on conflicts."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@lru_cache(maxsize=1)
def get_config() -> Dict:
    """
    Load base config then merge the active warehouse config on top.
    The result is cached for the lifetime of the process.
    """
    base = _load_yaml(CONFIG_DIR / "base.yaml")
    target = base.get("warehouse", {}).get("target", "snowflake")

    warehouse_cfg_path = CONFIG_DIR / f"{target}.yaml"
    if not warehouse_cfg_path.exists():
        raise FileNotFoundError(
            f"No config file found for warehouse target '{target}': {warehouse_cfg_path}"
        )

    warehouse_cfg = _load_yaml(warehouse_cfg_path)
    merged = _deep_merge(base, warehouse_cfg)
    return _process_values(merged)


def get_warehouse_target() -> str:
    return get_config()["warehouse"]["target"]

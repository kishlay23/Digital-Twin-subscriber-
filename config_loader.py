"""Config loader utilities: read_config, parse_ts, normalize_hw."""

import logging
import os
import json
from datetime import datetime
from typing import Any, Dict, Optional

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # type: ignore

logger = logging.getLogger(__name__)


def read_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    if yaml is not None:
        try:
            cfg = yaml.safe_load(text)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            logger.warning("YAML parsing failed; will try JSON.", exc_info=True)

    try:
        cfg = json.loads(text)
        if isinstance(cfg, dict):
            return cfg
        raise ValueError("Config parsed but did not produce a mapping (dict).")
    except Exception:
        logger.exception("JSON parsing failed for config file: %s", path)
        raise


def parse_ts(ts: Optional[str]) -> datetime:
    if not ts:
        logger.warning("No timestamp provided; using current time.")
        return datetime.now()
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        logger.exception("Failed to parse timestamp: %s; using current time.", ts)
        return datetime.now()


def normalize_hw(hw: Optional[str]) -> Optional[str]:
    if hw is None:
        return None
    try:
        hw = str(hw).strip().upper()
        if hw.startswith("0X"):
            hw = hw[2:]
        return hw
    except Exception:
        logger.exception("Failed to normalize hardware id: %r", hw)
        return None

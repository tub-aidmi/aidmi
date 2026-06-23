"""Human-readable run identifiers for Postgres schemas, filesystem paths, and results."""
from __future__ import annotations

import string
from typing import Any

from ulid import ULID

MAX_RUN_ID_LEN = 63


def slug(value: Any) -> str:
    s = str(value).lower()
    safe = "".join(
        ch if ch in (string.ascii_lowercase + string.digits + "_") else "_" for ch in s
    )
    return safe.strip("_") or "val"


def _truncate_run_id(run_prefix: str, strategy: str, fixture: str) -> str:
    prefix = f"{run_prefix}_"
    suffix = f"_{fixture}"
    budget = MAX_RUN_ID_LEN - len(prefix) - len(suffix)
    if budget < 1:
        return f"{run_prefix}_{fixture}"[:MAX_RUN_ID_LEN].rstrip("_")
    if len(strategy) <= budget:
        return f"{prefix}{strategy}{suffix}"
    truncated = strategy[:budget].rstrip("_")
    return f"{prefix}{truncated}{suffix}"


def make_run_id(strategy_name: str, fixture_name: str) -> str:
    ulid = str(ULID())
    hash8 = ulid[-8:].lower()
    run_prefix = f"r{hash8}"
    strategy = slug(strategy_name)
    fixture = slug(fixture_name)
    return _truncate_run_id(run_prefix, strategy, fixture)

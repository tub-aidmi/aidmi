"""Post-proposal dbt validation loop for structured strategies."""
from __future__ import annotations

from typing import Any, Awaitable, Callable

from aidmi_orchestrator.strategy.base import run_coroutines


async def retry_failing_tables(
    run_dbt: Callable[[], Awaitable[Any]],
    regenerate: Callable[[str, str], Awaitable[None]],
    *,
    max_passes: int,
    serial: bool = False,
) -> bool:
    """Run dbt up to max_passes times; between runs, regenerate only failing tables.

    regenerate(table_name, error_message) must rewrite that table's SQL on disk.
    Returns True as soon as dbt reports overall success. A dbt crash, a
    regeneration crash, or a failed run without named failing models all
    return False rather than raising.
    """
    for attempt in range(max_passes):
        try:
            result = await run_dbt()
        except Exception:
            return False
        if getattr(result, "overall_status", None) == "success":
            return True
        if attempt >= max_passes - 1:
            return False
        failing = [
            (m.model_name, m.error_message or m.status)
            for m in getattr(result, "models", []) or []
            if getattr(m, "status", None) != "success"
        ]
        if not failing:
            return False
        try:
            await run_coroutines(
                [regenerate(name, err) for name, err in failing],
                serial=serial,
            )
        except Exception:
            return False
    return False

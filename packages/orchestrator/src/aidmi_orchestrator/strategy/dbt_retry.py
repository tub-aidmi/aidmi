"""Shared dbt self-correction loop for structured strategies."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from aidmi_orchestrator.strategy.base import run_coroutines
from aidmi_pipeline.migration import dbt_model_table_name


def extract_failing_models(result: Any) -> list[tuple[str, str]]:
    failing: list[tuple[str, str]] = []
    for model in getattr(result, "models", []) or []:
        if getattr(model, "status", None) == "success":
            continue
        name = dbt_model_table_name(getattr(model, "model_name", "") or "<unknown>")
        message = getattr(model, "error_message", None) or getattr(
            model, "status", "error"
        )
        failing.append((name, str(message)))
    return failing


def summarize_dbt_failure(result: Any) -> str:
    lines: list[str] = []
    overall = getattr(result, "overall_status", None)
    if overall:
        lines.append(f"overall_status: {overall}")
    for name, message in extract_failing_models(result):
        lines.append(f"- {name}: {message}")
    return "\n".join(lines) if lines else "dbt run failed with no model details"


def failures_for_retry(
    result: Any,
    all_table_names: list[str] | None,
) -> list[tuple[str, str]]:
    """Collect explicit failures plus tables not successfully materialized."""
    failing = extract_failing_models(result)
    if not all_table_names:
        return failing

    known = {name for name, _ in failing}
    succeeded = {
        dbt_model_table_name(getattr(m, "model_name", "") or "<unknown>")
        for m in getattr(result, "models", []) or []
        if getattr(m, "status", None) == "success"
    }
    summary = summarize_dbt_failure(result)
    for name in all_table_names:
        if name not in succeeded and name not in known:
            failing.append((name, summary))
    return failing


async def retry_failing_tables(
    run_dbt: Callable[[], Awaitable[Any]],
    regenerate: Callable[[str, str], Awaitable[None]],
    *,
    max_passes: int,
    serial: bool = False,
    all_table_names: list[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> bool:
    """Run dbt up to max_passes; regenerate failing tables between attempts."""
    for attempt in range(max_passes):
        if progress_callback:
            progress_callback(attempt + 1, max_passes)

        try:
            result = await run_dbt()
        except Exception as exc:
            if attempt >= max_passes - 1 or not all_table_names:
                return False
            try:
                await run_coroutines(
                    [regenerate(name, repr(exc)) for name in all_table_names],
                    serial=True,
                )
            except Exception:
                return False
            continue

        if getattr(result, "overall_status", None) == "success":
            return True

        if attempt >= max_passes - 1:
            return False

        failing = failures_for_retry(result, all_table_names)
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

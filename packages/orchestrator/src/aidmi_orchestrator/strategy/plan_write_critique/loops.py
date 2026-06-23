"""Loop orchestration for plan_write_critique: dbt self-correction and critique rounds."""
from __future__ import annotations

from typing import Any, Awaitable, Callable

from aidmi_orchestrator.strategy.base import run_coroutines
from aidmi_orchestrator.strategy.structured_common import TableMapping
from aidmi_orchestrator.strategy.write_then_critique.critique import CritiqueReport


async def retry_failing_tables_with_progress(
    run_dbt: Callable[[], Awaitable[Any]],
    regenerate: Callable[[str, str], Awaitable[None]],
    *,
    max_passes: int,
    serial: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
) -> bool:
    for attempt in range(max_passes):
        if progress_callback:
            progress_callback(attempt + 1, max_passes)

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


async def run_critique_with_dbt_loop(
    mappings: dict[str, TableMapping],
    critique: Callable[[dict[str, TableMapping]], Awaitable[CritiqueReport]],
    revise: Callable[[str, TableMapping, str], Awaitable[TableMapping]],
    run_dbt_correction: Callable[[dict[str, TableMapping]], Awaitable[bool]],
    *,
    max_critique_rounds: int,
    serial: bool = False,
    log_progress: Callable[[str], None] | None = None,
) -> tuple[dict[str, TableMapping], bool]:
    log = log_progress or (lambda _: None)
    current = dict(mappings)

    log("=== PHASE 4: CRITIQUE LOOP ===")

    for round_num in range(max_critique_rounds):
        log(f"--- Critique round {round_num + 1}/{max_critique_rounds} ---")
        log("  Calling critic model (with query_postgres tool)...")
        report = await critique(current)

        rejected = {
            v.target_table: v.comments
            for v in report.verdicts
            if v.verdict == "needs_revision" and v.target_table in current
        }

        approved_count = len(current) - len(rejected)
        log(f"  Critique complete: {approved_count} approved, {len(rejected)} need revision")

        if rejected:
            log(f"    Rejected tables: {', '.join(rejected.keys())}")

        if not rejected:
            log("  All tables approved!")
            return current, True

        log(f"  Revising {len(rejected)} tables...")
        try:
            revised = await run_coroutines(
                [revise(name, current[name], comments) for name, comments in rejected.items()],
                serial=serial,
            )
        except Exception as e:
            log(f"  Revision failed: {e}")
            return current, False

        for name, m in zip(rejected, revised):
            current[name] = m
        log("  Revision complete")

        log("  Running dbt self-correction on revised tables...")
        dbt_ok = await run_dbt_correction(current)
        log(f"  dbt correction {'PASSED' if dbt_ok else 'FAILED or incomplete'}")

    log(f"Critique loop exhausted ({max_critique_rounds} rounds)")
    return current, False

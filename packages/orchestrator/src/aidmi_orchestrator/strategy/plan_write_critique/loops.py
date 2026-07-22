"""Loop orchestration for plan_write_critique: dbt self-correction and critique rounds."""

from __future__ import annotations

from typing import Awaitable, Callable

from aidmi_orchestrator.strategy.base import run_coroutines, run_named_coroutines
from aidmi_orchestrator.strategy.dbt_retry import retry_failing_tables
from aidmi_orchestrator.strategy.structured_common import TableMapping
from aidmi_orchestrator.strategy.write_then_critique.critique import CritiqueReport


async def retry_failing_tables_with_progress(
    run_dbt,
    regenerate,
    *,
    max_passes: int,
    serial: bool = False,
    all_table_names: list[str] | None = None,
    progress_callback=None,
) -> bool:
    return await retry_failing_tables(
        run_dbt,
        regenerate,
        max_passes=max_passes,
        serial=serial,
        all_table_names=all_table_names,
        progress_callback=progress_callback,
    )


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
        try:
            report = await critique(current)
        except Exception as e:
            log(f"  Critique failed: {e}")
            return current, False

        rejected = {
            v.target_table: v.comments
            for v in report.verdicts
            if v.verdict == "needs_revision" and v.target_table in current
        }

        approved_count = len(current) - len(rejected)
        log(
            f"  Critique complete: {approved_count} approved, {len(rejected)} need revision"
        )

        if rejected:
            log(f"    Rejected tables: {', '.join(rejected.keys())}")

        if not rejected:
            log("  All tables approved!")
            return current, True

        log(f"  Revising {len(rejected)} tables...")
        try:
            revised_map = await run_named_coroutines(
                [
                    (name, revise(name, current[name], comments))
                    for name, comments in rejected.items()
                ],
                serial=serial,
            )
        except Exception as e:
            log(f"  Revision failed: {e}")
            return current, False

        for name, m in revised_map.items():
            current[name] = m
        log("  Revision complete")

        log("  Running dbt self-correction on revised tables...")
        dbt_ok = await run_dbt_correction(current)
        log(f"  dbt correction {'PASSED' if dbt_ok else 'FAILED or incomplete'}")
        if not dbt_ok:
            log("  Stopping critique loop — dbt self-correction did not succeed")
            return current, False

    log(f"Critique loop exhausted ({max_critique_rounds} rounds)")
    return current, False

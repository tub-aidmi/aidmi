"""Shared dbt self-correction loop for structured mapping strategies."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.exceptions import UnexpectedModelBehavior

from aidmi_orchestrator.strategy.base import write_proposal, run_coroutines
from aidmi_orchestrator.strategy.dbt_retry import retry_failing_tables
from aidmi_orchestrator.strategy.structured_common import TableMapping, retry_user_prompt
from aidmi_orchestrator.strategy.validation import validate_models


async def run_dbt_self_correction(
    api: Any,
    agent: Agent,
    mappings_by_table: dict[str, TableMapping],
    context: str,
    *,
    dbt_project_path: Path,
    source_tables: list[tuple[str, str]],
    source_schema: str,
    max_passes: int,
    serial: bool,
    run_kwargs: dict | None = None,
    fixer_agent: Agent | None = None,
    fixer_run_kwargs: dict | None = None,
    validation_gate: str = "none",
) -> bool:
    """Run dbt up to max_passes; regenerate failing tables between attempts.

    When ``fixer_agent`` is set, repairs route to it (with ``fixer_run_kwargs``)
    instead of the writer ``agent``. When ``validation_gate`` is "static" (or
    "static+explain"), a cheap sqlglot pass fixes malformed SQL before any dbt run.
    """
    fixer = fixer_agent or agent
    fixer_kwargs = fixer_run_kwargs if fixer_agent is not None else run_kwargs

    async def regenerate(table_name: str, error_message: str) -> None:
        previous = mappings_by_table.get(table_name)
        prompt = retry_user_prompt(
            table_name,
            context,
            previous.dbt_sql if previous else "",
            error_message,
        )
        try:
            run = await fixer.run(prompt, **(fixer_kwargs or {}))
        except UnexpectedModelBehavior:
            return
        fixed = run.output.model_copy(update={"target_table": table_name})
        mappings_by_table[table_name] = fixed
        write_proposal(
            dbt_project_path,
            {name: m.dbt_sql for name, m in mappings_by_table.items()},
            source_tables,
            source_schema,
        )

    if validation_gate in ("static", "static+explain"):
        for _ in range(max_passes):
            errors = validate_models(
                {name: m.dbt_sql for name, m in mappings_by_table.items()}
            )
            if not errors:
                break
            await run_coroutines(
                [regenerate(name, "; ".join(errs)) for name, errs in errors.items()],
                serial=serial,
            )

    return await retry_failing_tables(
        api.run_dbt,
        regenerate,
        max_passes=max_passes,
        serial=serial,
        all_table_names=list(mappings_by_table),
    )

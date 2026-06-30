"""Shared dbt self-correction loop for structured mapping strategies."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic_ai import Agent

from aidmi_orchestrator.strategy.base import write_proposal
from aidmi_orchestrator.strategy.dbt_retry import retry_failing_tables
from aidmi_orchestrator.strategy.structured_common import TableMapping, retry_user_prompt


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
) -> bool:
    """Run dbt up to max_passes; regenerate failing tables between attempts."""

    async def regenerate(table_name: str, error_message: str) -> None:
        previous = mappings_by_table.get(table_name)
        prompt = retry_user_prompt(
            table_name,
            context,
            previous.dbt_sql if previous else "",
            error_message,
        )
        run = await agent.run(prompt, **(run_kwargs or {}))
        fixed = run.output.model_copy(update={"target_table": table_name})
        mappings_by_table[table_name] = fixed
        write_proposal(
            dbt_project_path,
            {name: m.dbt_sql for name, m in mappings_by_table.items()},
            source_tables,
            source_schema,
        )

    return await retry_failing_tables(
        api.run_dbt,
        regenerate,
        max_passes=max_passes,
        serial=serial,
        all_table_names=list(mappings_by_table),
    )

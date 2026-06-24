"""Post-agent dbt validation loop for write_tools_freeform."""
from __future__ import annotations

from typing import Any, Protocol

from pydantic_ai import Agent, UsageLimits

from aidmi_pipeline.sources_yaml import ensure_sources_yaml_raw_schema


class _DbtRunnable(Protocol):
    source_schema: str
    dbt_project_path: Any

    async def run_dbt(self) -> Any: ...


def format_dbt_errors(result: Any) -> str:
    if result is None:
        return "dbt run returned no result"
    lines: list[str] = []
    overall = getattr(result, "overall_status", None)
    if overall:
        lines.append(f"overall_status: {overall}")
    for model in getattr(result, "models", []) or []:
        if getattr(model, "status", None) != "success":
            name = getattr(model, "model_name", "?")
            msg = getattr(model, "error_message", None) or getattr(model, "status", "error")
            lines.append(f"- {name}: {msg}")
    return "\n".join(lines) if lines else "dbt run failed with no model details"


async def run_post_agent_dbt_loop(
    api: _DbtRunnable,
    agent: Agent,
    usage_limits: UsageLimits,
    *,
    max_passes: int,
    run_kwargs: dict | None = None,
) -> bool:
    """Run dbt after the agent finishes; on failure, prompt the agent to fix SQL."""
    models_dir = api.dbt_project_path / "models"
    if not models_dir.exists() or not list(models_dir.glob("*.sql")):
        return False

    correction_intro = (
        "dbt run failed after your last edit. Fix every failing model using valid "
        "PostgreSQL only (no TRY_CAST or invented functions). read_file each model "
        "you change. The orchestrator will re-run dbt automatically."
    )

    for attempt in range(max_passes):
        ensure_sources_yaml_raw_schema(models_dir, api.source_schema)
        try:
            result = await api.run_dbt()
            if getattr(result, "overall_status", None) == "success":
                return True
            error_text = format_dbt_errors(result)
        except Exception as e:
            error_text = repr(e)

        if attempt >= max_passes - 1:
            return False

        await agent.run(
            f"{correction_intro}\n\nErrors:\n{error_text}",
            usage_limits=usage_limits,
            **(run_kwargs or {}),
        )

    return False

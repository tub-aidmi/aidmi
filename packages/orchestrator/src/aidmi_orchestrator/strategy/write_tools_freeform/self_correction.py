"""Post-agent dbt validation loop for write_tools_freeform."""

from __future__ import annotations

from typing import Any, Protocol

from pydantic_ai import Agent, UsageLimits
from pydantic_ai.exceptions import ModelHTTPError, UnexpectedModelBehavior

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
            msg = getattr(model, "error_message", None) or getattr(
                model, "status", "error"
            )
            lines.append(f"- {name}: {msg}")
    return "\n".join(lines) if lines else "dbt run failed with no model details"


async def run_post_agent_dbt_loop(
    api: _DbtRunnable,
    agent: Agent,
    usage_limits: UsageLimits,
    *,
    max_passes: int,
    run_kwargs: dict | None = None,
    fixer_agent: Agent | None = None,
    fixer_run_kwargs: dict | None = None,
    validation_gate: str = "none",
) -> bool:
    """Run dbt after the agent finishes; on failure, prompt an agent to fix SQL.

    Repairs route to ``fixer_agent`` when set. When ``validation_gate`` is static,
    a sqlglot pass fixes malformed model files before the first dbt run.
    """
    from aidmi_orchestrator.strategy.validation import validate_models

    models_dir = api.dbt_project_path / "models"
    if not models_dir.exists() or not list(models_dir.glob("*.sql")):
        return False

    fixer = fixer_agent or agent
    fixer_kwargs = fixer_run_kwargs if fixer_agent is not None else run_kwargs

    correction_intro = (
        "dbt run failed after your last edit. Fix every failing model using valid "
        "PostgreSQL only (no TRY_CAST or invented functions). read_file each model "
        "you change. The orchestrator will re-run dbt automatically."
    )

    if validation_gate in ("static", "static+explain"):
        for _ in range(max_passes):
            sql_by_file = {
                p.stem: p.read_text(encoding="utf-8") for p in models_dir.glob("*.sql")
            }
            errors = validate_models(sql_by_file)
            if not errors:
                break
            detail = "\n".join(
                f"- {name}: {'; '.join(errs)}" for name, errs in errors.items()
            )
            try:
                await fixer.run(
                    f"Some models have SQL syntax errors before dbt even runs. "
                    f"read_file and fix each one with valid PostgreSQL:\n{detail}",
                    usage_limits=usage_limits,
                    **(fixer_kwargs or {}),
                )
            except (UnexpectedModelBehavior, ModelHTTPError):
                break

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

        try:
            await fixer.run(
                f"{correction_intro}\n\nErrors:\n{error_text}",
                usage_limits=usage_limits,
                **(fixer_kwargs or {}),
            )
        except (UnexpectedModelBehavior, ModelHTTPError):
            return False

    return False

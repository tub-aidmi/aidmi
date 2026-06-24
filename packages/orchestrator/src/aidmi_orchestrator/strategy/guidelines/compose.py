"""Compose modular guideline sections into strategy prompts."""
from __future__ import annotations

from aidmi_orchestrator.strategy.guidelines.dbt import DBT_PROJECT_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.planning import PLANNING_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.postgres import POSTGRES_SQL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.query_tool import QUERY_TOOL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.transformation import TRANSFORMATION_GUIDELINES

FREEFORM_SELF_CORRECTION_ADDENDUM = """\
## Self-correction

- You MUST call run_dbt() after writing or editing models and before declaring the project complete.
- If run_dbt() reports errors, read the failing model files, fix the SQL, and call run_dbt() again until it succeeds or you cannot fix the errors.
- Do NOT skip run_dbt() to save time — the orchestrator will run dbt again and fail if SQL is invalid.
- When you change one model (e.g. casting or date logic), apply the same fix to every model that uses the same pattern.
"""


def join_sections(*parts: str) -> str:
    return "\n\n".join(p.strip() for p in parts if p.strip())


def context_transformation_section() -> str:
    return TRANSFORMATION_GUIDELINES


def retry_correction_reminder() -> str:
    return join_sections(
        "When fixing errors:",
        "- Ensure every mixed-case output column uses a double-quoted alias: `expr AS \"ColumnName\"`.",
        "- Ensure every CAST includes `AS <type>`: `CAST(expr AS DOUBLE PRECISION)`, not `CAST(expr)`.",
        POSTGRES_SQL_GUIDELINES,
    )


def writer_system_prompt(role_preamble: str) -> str:
    return join_sections(
        role_preamble,
        DBT_PROJECT_GUIDELINES,
        POSTGRES_SQL_GUIDELINES,
        TRANSFORMATION_GUIDELINES,
        QUERY_TOOL_GUIDELINES,
    )


def planner_system_prompt(role_preamble: str) -> str:
    return join_sections(
        role_preamble,
        PLANNING_GUIDELINES,
        TRANSFORMATION_GUIDELINES,
    )


def freeform_system_prompt(*, enable_self_correction: bool = False) -> str:
    role = (
        "You are a senior data engineer producing a dbt project on disk.\n\n"
        "You have these tools:\n"
        "- write_file(path, content) — write a file under the dbt project dir\n"
        "- read_file(path) — read a file you've written\n"
        "- (optionally) query_postgres(sql) — run a read-only SELECT against staging\n"
        "- (optionally) run_dbt() — execute the dbt project and see the result\n\n"
        "Layout you must produce:\n"
        "- models/<target_table>.sql — one per target table\n"
        "- models/sources.yml — declare every source used by your models\n\n"
        "Raw source data lives in the physical Postgres source schema shown in context. "
        "Transformed dbt models materialize into a per-run output schema at run time."
    )
    parts = [
        role,
        DBT_PROJECT_GUIDELINES,
        POSTGRES_SQL_GUIDELINES,
        TRANSFORMATION_GUIDELINES,
        QUERY_TOOL_GUIDELINES,
        "Stop when the dbt project is complete.",
    ]
    if enable_self_correction:
        parts.append(FREEFORM_SELF_CORRECTION_ADDENDUM)
    return join_sections(*parts)


def critic_system_prompt(role_preamble: str, *, with_query_tool: bool = False) -> str:
    parts = [role_preamble, POSTGRES_SQL_GUIDELINES, TRANSFORMATION_GUIDELINES]
    if with_query_tool:
        parts.append(QUERY_TOOL_GUIDELINES)
    return join_sections(*parts)


def judge_system_prompt(role_preamble: str) -> str:
    return join_sections(
        role_preamble,
        POSTGRES_SQL_GUIDELINES,
        TRANSFORMATION_GUIDELINES,
    )

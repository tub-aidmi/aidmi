"""Compose modular guideline sections into strategy prompts."""
from __future__ import annotations

from aidmi_orchestrator.strategy.guidelines.dbt import DBT_PROJECT_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.planning import PLANNING_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.postgres import POSTGRES_SQL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.query_tool import QUERY_TOOL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.transformation import TRANSFORMATION_GUIDELINES

FREEFORM_INLINE_SELF_CORRECTION_ADDENDUM = """\
## Self-correction

- You MUST call run_dbt() after writing or editing models and before declaring the project complete.
- If run_dbt() reports errors, read the failing model files, fix the SQL, and call run_dbt() again until it succeeds or you cannot fix the errors.
- Do NOT skip run_dbt() to save time — the orchestrator will run dbt again and fail if SQL is invalid.
- When you change one model (e.g. casting or date logic), apply the same fix to every model that uses the same pattern.
"""

FREEFORM_POST_AGENT_SELF_CORRECTION_ADDENDUM = """\
## Self-correction

- Write complete, valid PostgreSQL dbt models before finishing — do not rely on mid-run dbt checks.
- The orchestrator runs dbt after you stop; if models fail, it will ask you to fix SQL in a follow-up pass.
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
        "- Populate every `Legacy_*__c` column from the source natural key — evaluators join on these.",
        "- `AccountId` / `Account__c` must reference the Salesforce Account `Id`, not raw source customer numbers.",
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


def freeform_system_prompt(
    *,
    enable_self_correction: bool = False,
    inline_run_dbt_tool: bool = False,
) -> str:
    tool_lines = [
        "- write_file(path, content) — write a file under the dbt project dir",
        "- read_file(path) — read a file you've written",
        "- (optionally) query_postgres(sql) — run a read-only SELECT against staging",
    ]
    if enable_self_correction and inline_run_dbt_tool:
        tool_lines.append("- (optionally) run_dbt() — execute the dbt project and see the result")
    role = (
        "You are a senior data engineer producing a dbt project on disk.\n\n"
        "You have these tools:\n"
        + "\n".join(tool_lines)
        + "\n\n"
        "Layout you must produce:\n"
        "- models/<target_table>.sql — one per target table (paths are relative to the dbt project root, not dbt_project/models/)\n"
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
        if inline_run_dbt_tool:
            parts.append(FREEFORM_INLINE_SELF_CORRECTION_ADDENDUM)
        else:
            parts.append(FREEFORM_POST_AGENT_SELF_CORRECTION_ADDENDUM)
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

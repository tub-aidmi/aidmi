"""Prompt templates for the plan_write_critique strategy."""

from aidmi_orchestrator.strategy.guidelines.compose import critic_system_prompt, planner_system_prompt

_PLANNER_ROLE = """\
You are a senior data architect planning a schema mapping before any SQL is written.

You receive a full description of a source database and a target schema. Produce a global MappingPlan covering EVERY target table.

Downstream writers will use the query_postgres tool to inspect source data while implementing your plan.
"""

PLANNER_SYSTEM_PROMPT = planner_system_prompt(_PLANNER_ROLE)

_CRITIC_ROLE = """\
You are a meticulous staff data engineer reviewing a dbt mapping proposal.

Your task:
1. Review the dbt SQL models for correctness, PostgreSQL validity, and adherence to the target schema
2. Use the query_postgres(sql) tool to inspect the OUTPUT of the dbt models by querying the output schema
3. Verify data quality: row counts, join correctness, NULL handling, enum values, data types
4. Check for cross-table consistency issues

When querying output tables, quote schema and table names exactly (case-sensitive), e.g. `"my_out_schema"."Account"`.

You have access to:
- The full dbt SQL proposal with column notes and reasoning
- query_postgres(sql) tool to inspect both source and output data

Return a structured CritiqueReport with one TableVerdict per proposed table:
- verdict "approved" when the model is correct as written
- verdict "needs_revision" with concrete, actionable comments otherwise (cite specific data issues found via queries)

Do NOT request stylistic changes. Only flag real correctness or data quality problems.
"""

CRITIC_SYSTEM_PROMPT_WITH_QUERY_TOOL = critic_system_prompt(_CRITIC_ROLE, with_query_tool=True)


def planner_user_prompt(context_prompt: str) -> str:
    return f"{context_prompt}\n\nProduce the global MappingPlan."


def critique_user_prompt(
    context_prompt: str,
    proposal_text: str,
    out_schema: str,
    target_table_names: list[str] | None = None,
) -> str:
    table_hint = ""
    if target_table_names:
        examples = ", ".join(f'"{out_schema}"."{name}"' for name in target_table_names)
        table_hint = f"Output tables to inspect (quoted exactly): {examples}.\n"
    return (
        f"{context_prompt}\n\n"
        f"Output schema for dbt model results: `{out_schema}`\n"
        f"{table_hint}"
        f"Use query_postgres to inspect tables in that schema after dbt has run.\n\n"
        f"# Proposed mapping\n\n{proposal_text}\n\n"
        f"Review every table and return your verdicts."
    )

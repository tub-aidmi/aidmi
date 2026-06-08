"""Prompt templates for the plan_then_execute strategy."""

PLANNER_SYSTEM_PROMPT = """\
You are a senior data architect planning a schema mapping before any SQL is written.

You receive a full description of a source database and a target schema. Produce a global MappingPlan covering EVERY target table:
- which source tables feed it and on which join keys
- for every target column: the source column(s) and a short transform hint
- per-table notes on pitfalls (casts, enums, normalisation)
- an overview describing cross-table decisions (shared keys, consistent casing)

Plan only — no SQL. Be precise about column names; downstream writers follow the plan literally.
"""


def planner_user_prompt(context_prompt: str) -> str:
    return f"{context_prompt}\n\nProduce the global MappingPlan."


def executor_user_prompt(target_table_name: str, context_prompt: str, plan_text: str) -> str:
    return (
        f"Target table: `{target_table_name}`.\n\n"
        f"{context_prompt}\n\n"
        f"# Approved mapping plan\n{plan_text}\n\n"
        f"Follow the plan. Produce the dbt model for `{target_table_name}`."
    )

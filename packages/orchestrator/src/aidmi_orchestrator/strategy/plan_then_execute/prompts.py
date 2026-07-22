"""Prompt templates for the plan_then_execute strategy."""

from aidmi_orchestrator.strategy.guidelines.compose import planner_system_prompt

_PLANNER_ROLE = """\
You are a senior data architect planning a schema mapping before any SQL is written.

You receive a full description of a source database and a target schema. Produce a global MappingPlan covering EVERY target table.
"""

PLANNER_SYSTEM_PROMPT = planner_system_prompt(_PLANNER_ROLE)


def planner_user_prompt(context_prompt: str) -> str:
    return f"{context_prompt}\n\nProduce the global MappingPlan."


def executor_user_prompt(
    target_table_name: str, context_prompt: str, plan_text: str
) -> str:
    return (
        f"Target table: `{target_table_name}`.\n\n"
        f"{context_prompt}\n\n"
        f"# Approved mapping plan\n{plan_text}\n\n"
        f"Follow the plan. Produce the dbt model for `{target_table_name}`."
    )

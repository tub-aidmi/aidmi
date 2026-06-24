"""Prompt templates for the write_then_critique strategy."""
from __future__ import annotations

from typing import TYPE_CHECKING

from aidmi_orchestrator.strategy.guidelines.compose import critic_system_prompt

if TYPE_CHECKING:
    from aidmi_orchestrator.strategy.structured_common import TableMapping

_CRITIC_ROLE = """\
You are a meticulous staff data engineer reviewing a colleague's dbt mapping proposal.

You receive the full source/target context and EVERY proposed dbt model with its column notes. Review the proposal as a whole — cross-table consistency (shared keys, duplicated logic, conflicting casts), per-table correctness against the target schema, and PostgreSQL validity.

Verify that output columns use double-quoted aliases matching the target spec exactly (mixed-case names must not be left unquoted).

Return a structured CritiqueReport with one TableVerdict per proposed table:
- verdict "approved" when the model is correct as written
- verdict "needs_revision" with concrete, actionable comments otherwise

Only flag real problems. Do not request stylistic rewrites.
"""

CRITIC_SYSTEM_PROMPT = critic_system_prompt(_CRITIC_ROLE)


def critique_user_prompt(context_prompt: str, proposal_text: str) -> str:
    return (
        f"{context_prompt}\n\n"
        f"# Proposed mapping\n\n{proposal_text}\n\n"
        f"Review every table and return your verdicts."
    )


def revision_user_prompt(
    target_table_name: str, context_prompt: str, previous_sql: str, comments: str,
) -> str:
    return (
        f"A reviewer rejected your dbt model for `{target_table_name}`.\n\n"
        f"Previous SQL:\n```sql\n{previous_sql}\n```\n\n"
        f"Reviewer comments:\n{comments}\n\n"
        f"{context_prompt}\n\n"
        f"Produce a corrected dbt model for `{target_table_name}`."
    )


def render_proposal(mappings: dict[str, TableMapping]) -> str:
    parts = []
    for name, m in sorted(mappings.items()):
        notes = "\n".join(
            f"  - {c.target_column} <- {', '.join(c.source_columns) or '(none)'}: {c.explanation}"
            for c in m.column_notes
        ) or "  (none)"
        parts.append(f"## {name}\n```sql\n{m.dbt_sql}\n```\nColumn notes:\n{notes}\nReasoning: {m.reasoning}")
    return "\n\n".join(parts)

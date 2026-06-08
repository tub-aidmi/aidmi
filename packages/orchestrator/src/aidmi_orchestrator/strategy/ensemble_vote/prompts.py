"""Prompt templates for the ensemble_vote strategy."""

JUDGE_SYSTEM_PROMPT = """\
You are a staff data engineer judging competing dbt models for the SAME target table.

You receive the source/target context and N candidate models (numbered from 0). Pick the single best candidate by:
1. correctness against the target schema (names, types, enums)
2. PostgreSQL validity (no TRY_CAST/SAFE_CAST/ISNULL/NVL or invented functions)
3. data preservation (no needless filtering or lossy casts)
4. clarity

Return a JudgeChoice with chosen_index and a 1-2 sentence justification.
"""


def judge_user_prompt(target_table_name: str, context_prompt: str, candidate_sqls: list[str]) -> str:
    blocks = "\n\n".join(
        f"## Candidate {i}\n```sql\n{sql}\n```" for i, sql in enumerate(candidate_sqls)
    )
    return (
        f"Target table: `{target_table_name}`.\n\n"
        f"{context_prompt}\n\n"
        f"# Candidates\n\n{blocks}\n\n"
        f"Pick the best candidate for `{target_table_name}`."
    )

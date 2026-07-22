"""Critique round orchestration, separated for unit testing."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal

from pydantic import BaseModel, Field

from aidmi_orchestrator.strategy.base import run_coroutines
from aidmi_orchestrator.strategy.structured_common import TableMapping


class TableVerdict(BaseModel):
    target_table: str
    verdict: Literal["approved", "needs_revision"]
    comments: str = ""


class CritiqueReport(BaseModel):
    verdicts: list[TableVerdict] = Field(default_factory=list)


async def run_critique_rounds(
    mappings: dict[str, TableMapping],
    critique: Callable[[dict[str, TableMapping]], Awaitable[CritiqueReport]],
    revise: Callable[[str, TableMapping, str], Awaitable[TableMapping]],
    *,
    max_rounds: int,
    serial: bool = False,
) -> tuple[dict[str, TableMapping], bool]:
    """Returns (final mappings, all_approved).

    A crash inside revise() returns the current mappings unapproved rather
    than raising, mirroring the structured self-correction policy.
    """
    current = dict(mappings)
    for _ in range(max_rounds):
        report = await critique(current)
        rejected = {
            v.target_table: v.comments
            for v in report.verdicts
            if v.verdict == "needs_revision" and v.target_table in current
        }
        if not rejected:
            return current, True
        try:
            revised = await run_coroutines(
                [
                    revise(name, current[name], comments)
                    for name, comments in rejected.items()
                ],
                serial=serial,
            )
        except Exception:
            return current, False
        for name, m in zip(rejected, revised, strict=False):
            current[name] = m.model_copy(update={"target_table": name})
    return current, False

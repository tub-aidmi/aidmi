"""Shared registration helper for SQL-backed fixtures."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.fixtures.base import Fixture, register_fixture

DEFAULT_EVALUATORS = ["execution", "llm_usage", "schema"]


def register_sql_fixture(
    name: str,
    description: str,
    *,
    evaluators: list[str] | None = None,
) -> None:
    here = Path(__file__).parent / name
    register_fixture(
        Fixture(
            name=name,
            description=description,
            source_schema=f"fixture_{name}_src",
            source_sql_path=here / "source.sql",
            destination_sql_path=here / "destination.sql",
            target_schema_path=here / "target_schema.json",
            reference_dbt_path=None,
            applicable_evaluators=evaluators or DEFAULT_EVALUATORS,
        )
    )

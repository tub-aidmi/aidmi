"""MockStrategy: deterministic. Reads a JSON file describing a mapping and
writes the SQL files directly. No LLM calls. Used in integration tests and
as a baseline in benchmark sweeps.
"""
from __future__ import annotations
import json
from pathlib import Path
from pydantic import BaseModel

from aidmi_orchestrator.domain import (
    StrategyResult, MappingManifest, TableMappingNote, ColumnNote,
)
from aidmi_orchestrator.strategy.base import write_proposal


class MockStrategyConfig(BaseModel):
    mapping_source: str   # path to a mapping JSON


class MockStrategy:
    name = "mock"

    def __init__(self, config: MockStrategyConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        spec = json.loads(Path(self.config.mapping_source).read_text(encoding="utf-8"))
        sql_by_table: dict[str, str] = {}
        notes: list[TableMappingNote] = []
        source_tables: set[tuple[str, str]] = set()

        for target_table, entry in spec["tables"].items():
            sql_by_table[target_table] = entry["sql"]
            for s in entry.get("source_tables", []):
                source_tables.add(tuple(s))
            notes.append(TableMappingNote(
                target_table=target_table,
                source_tables=[s[1] for s in entry.get("source_tables", [])],
                column_notes=[ColumnNote(**c) for c in entry.get("column_notes", [])],
                reasoning=entry.get("reasoning", ""),
            ))

        write_proposal(api.dbt_project_path, sql_by_table, sorted(source_tables))

        manifest = MappingManifest(
            tables=notes,
            strategy_name=self.name,
            strategy_config=self.config.model_dump(),
        )
        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=manifest,
            self_reported_status="complete",
        )

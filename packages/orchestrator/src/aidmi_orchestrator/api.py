"""OrchestratorAPI — the surface every strategy receives."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from aidmi_orchestrator.domain import ModelSpec, SourceSummary, TargetSchema
from aidmi_orchestrator.llm import TracedModel, make_llm
from aidmi_orchestrator.trace import DbtRunEvent, TraceSink

_SELECT_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


@dataclass
class OrchestratorAPI:
    source_summary: SourceSummary
    target_schema: TargetSchema | None
    dbt_project_path: Path
    staging_db_url: str
    source_schema: str
    out_schema: str
    trace: TraceSink
    _pipeline_run: Any = None  # aidmi_pipeline.MigrationRun; set by orchestrator

    def make_llm(self, spec: ModelSpec, role: str = "main") -> Any:
        inner = make_llm(spec)
        return TracedModel(inner, spec, role, self.trace)

    async def run_dbt(self):
        from aidmi_pipeline.migration import transform

        if self._pipeline_run is None:
            raise RuntimeError("api.run_dbt() called but no pipeline_run wired in")
        start = time.perf_counter()
        result = await asyncio.to_thread(transform, self._pipeline_run)
        duration_ms = (time.perf_counter() - start) * 1000
        self.trace.record(
            DbtRunEvent(
                timestamp=datetime.utcnow(),
                transform_result=result.model_dump(),
                duration_ms=duration_ms,
            )
        )
        return result

    def read_table_sample(self, schema: str, table: str, n: int = 100) -> list[dict]:
        with psycopg2.connect(self.staging_db_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT %s', (n,))
                return [dict(r) for r in cur.fetchall()]

    def query_postgres(self, sql: str, row_cap: int = 1000) -> list[dict]:
        if not _SELECT_RE.match(sql):
            raise ValueError("only SELECT/WITH queries are allowed via query_postgres")
        with psycopg2.connect(self.staging_db_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows: list[dict] = []
                for r in cur:
                    if len(rows) >= row_cap:
                        break
                    rows.append(dict(r))
                return rows

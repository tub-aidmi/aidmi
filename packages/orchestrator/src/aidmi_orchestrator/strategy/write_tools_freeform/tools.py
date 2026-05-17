"""Tool function factories. Each returns a (name, async_callable) pair.

Tools close over the OrchestratorAPI so they can write/read files, query
Postgres, and call api.run_dbt() under strict containment.
"""
from __future__ import annotations
from pathlib import Path
from typing import Any
import time
from datetime import datetime

from aidmi_orchestrator.trace import ToolCallEvent


def _ensure_under(root: Path, candidate: str) -> Path:
    p = (root / candidate).resolve()
    root_abs = root.resolve()
    if root_abs not in p.parents and p != root_abs:
        raise ValueError(f"path {candidate!r} escapes the dbt project directory")
    return p


def make_write_file(api):
    async def write_file(path: str, content: str) -> str:
        start = time.perf_counter()
        target = _ensure_under(api.dbt_project_path, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(ToolCallEvent(
            timestamp=datetime.utcnow(), tool_name="write_file",
            args={"path": path, "size": len(content)}, result="ok", latency_ms=latency_ms,
        ))
        return f"wrote {len(content)} bytes to {path}"
    return write_file


def make_read_file(api):
    async def read_file(path: str) -> str:
        start = time.perf_counter()
        target = _ensure_under(api.dbt_project_path, path)
        content = target.read_text(encoding="utf-8") if target.exists() else ""
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(ToolCallEvent(
            timestamp=datetime.utcnow(), tool_name="read_file",
            args={"path": path}, result={"size": len(content)}, latency_ms=latency_ms,
        ))
        return content
    return read_file


def make_query_postgres(api, row_cap: int):
    async def query_postgres(sql: str) -> list[dict]:
        start = time.perf_counter()
        rows = api.query_postgres(sql, row_cap=row_cap)
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(ToolCallEvent(
            timestamp=datetime.utcnow(), tool_name="query_postgres",
            args={"sql": sql[:500], "row_cap": row_cap}, result={"rows": len(rows)}, latency_ms=latency_ms,
        ))
        return rows
    return query_postgres


def make_run_dbt(api):
    async def run_dbt() -> dict:
        result = await api.run_dbt()
        return result.model_dump()
    return run_dbt

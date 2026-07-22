"""Tool function factories. Each returns a (name, async_callable) pair.

Tools close over the OrchestratorAPI so they can write/read files, query
Postgres, and call api.run_dbt() under strict containment.
"""

from __future__ import annotations

import time
from pathlib import Path

import psycopg2

from aidmi_orchestrator.clock import utc_now
from aidmi_orchestrator.strategy.sql_sanitize import sanitize_dbt_sql, validate_dbt_sql
from aidmi_orchestrator.trace import ToolCallEvent


def _ensure_under(root: Path, candidate: str) -> Path:
    p = (root / candidate).resolve()
    root_abs = root.resolve()
    if root_abs not in p.parents and p != root_abs:
        raise ValueError(f"path {candidate!r} escapes the dbt project directory")
    return p


def _reject_nested_dbt_project_path(path: str) -> str | None:
    if "dbt_project" in Path(path).parts:
        return (
            f"ERROR: path {path!r} must not include 'dbt_project/'. "
            "Use models/<Table>.sql relative to the project root."
        )
    return None


def make_write_file(api):
    async def write_file(path: str, content: str) -> str:
        start = time.perf_counter()
        nested_err = _reject_nested_dbt_project_path(path)
        if nested_err:
            latency_ms = (time.perf_counter() - start) * 1000
            api.trace.record(
                ToolCallEvent(
                    timestamp=utc_now(),
                    tool_name="write_file",
                    args={"path": path, "size": len(content)},
                    result=nested_err,
                    latency_ms=latency_ms,
                )
            )
            return nested_err
        if path.endswith(".sql"):
            content = sanitize_dbt_sql(content)
            validation_err = validate_dbt_sql(content)
            if validation_err:
                latency_ms = (time.perf_counter() - start) * 1000
                msg = f"ERROR: {validation_err} Fix the SQL and try again."
                api.trace.record(
                    ToolCallEvent(
                        timestamp=utc_now(),
                        tool_name="write_file",
                        args={"path": path, "size": len(content)},
                        result=msg,
                        latency_ms=latency_ms,
                    )
                )
                return msg
        try:
            target = _ensure_under(api.dbt_project_path, path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        except (ValueError, OSError) as e:
            latency_ms = (time.perf_counter() - start) * 1000
            api.trace.record(
                ToolCallEvent(
                    timestamp=utc_now(),
                    tool_name="write_file",
                    args={"path": path, "size": len(content)},
                    result=f"error: {e!r}",
                    latency_ms=latency_ms,
                )
            )
            return f"ERROR: could not write {path!r}: {e}. Use a file path like models/<name>.sql, not a directory."
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(
            ToolCallEvent(
                timestamp=utc_now(),
                tool_name="write_file",
                args={"path": path, "size": len(content)},
                result="ok",
                latency_ms=latency_ms,
            )
        )
        return f"wrote {len(content)} bytes to {path}"

    return write_file


def make_read_file(api):
    async def read_file(path: str) -> str:
        start = time.perf_counter()
        try:
            target = _ensure_under(api.dbt_project_path, path)
            content = target.read_text(encoding="utf-8") if target.exists() else ""
        except (ValueError, OSError) as e:
            latency_ms = (time.perf_counter() - start) * 1000
            api.trace.record(
                ToolCallEvent(
                    timestamp=utc_now(),
                    tool_name="read_file",
                    args={"path": path},
                    result=f"error: {e!r}",
                    latency_ms=latency_ms,
                )
            )
            return f"ERROR: could not read {path!r}: {e}. Use a file path like models/<name>.sql, not a directory."
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(
            ToolCallEvent(
                timestamp=utc_now(),
                tool_name="read_file",
                args={"path": path},
                result={"size": len(content)},
                latency_ms=latency_ms,
            )
        )
        return content

    return read_file


def make_query_postgres(api, row_cap: int):
    async def query_postgres(sql: str) -> list[dict]:
        start = time.perf_counter()
        try:
            rows = api.query_postgres(sql, row_cap=row_cap)
        except ValueError as e:
            latency_ms = (time.perf_counter() - start) * 1000
            msg = str(e)
            api.trace.record(
                ToolCallEvent(
                    timestamp=utc_now(),
                    tool_name="query_postgres",
                    args={"sql": sql[:500], "row_cap": row_cap},
                    result={"error": msg},
                    latency_ms=latency_ms,
                )
            )
            return [{"error": msg}]
        except psycopg2.Error as e:
            latency_ms = (time.perf_counter() - start) * 1000
            msg = str(e).strip()
            if "{{" in sql or "source(" in sql:
                msg += (
                    " Hint: query_postgres expects plain PostgreSQL "
                    '(e.g. SELECT * FROM "schema"."table") — not dbt {{ source(...) }}.'
                )
            api.trace.record(
                ToolCallEvent(
                    timestamp=utc_now(),
                    tool_name="query_postgres",
                    args={"sql": sql[:500], "row_cap": row_cap},
                    result={"error": msg},
                    latency_ms=latency_ms,
                )
            )
            return [{"error": msg}]
        latency_ms = (time.perf_counter() - start) * 1000
        api.trace.record(
            ToolCallEvent(
                timestamp=utc_now(),
                tool_name="query_postgres",
                args={"sql": sql[:500], "row_cap": row_cap},
                result={"rows": len(rows)},
                latency_ms=latency_ms,
            )
        )
        return rows

    return query_postgres


def make_run_dbt(api, max_passes: int = 3):
    counter = {"n": 0}

    async def run_dbt() -> dict:
        if counter["n"] >= max_passes:
            return {"error": f"max_self_correction_passes={max_passes} reached"}
        counter["n"] += 1
        try:
            result = await api.run_dbt()
            return result.model_dump()
        except Exception as e:
            return {"error": repr(e), "overall_status": "error"}

    return run_dbt

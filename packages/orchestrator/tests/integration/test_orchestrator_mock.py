"""Integration test: full orchestrator end-to-end with MockStrategy.

Asserts the success criteria from spec Section 2 point 1.
"""
import json
import asyncio
from pathlib import Path
import pytest
import psycopg2

from aidmi_pipeline.config import staging_schemas_for_run

# Trigger registrations
import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy
from aidmi_orchestrator.benchmark import Benchmark


def test_mock_strategy_full_pipeline(staging_db_url, tmp_path):
    fixture = get_fixture("sp1_users")
    mapping_source = str(fixture.target_schema_path.parent / "mock_mapping.json")
    strategy = make_strategy("mock", {"mapping_source": mapping_source})

    bench = Benchmark(fixture, workspace=tmp_path, staging_db_url=staging_db_url)
    result = asyncio.run(bench.run(strategy))

    # Success criteria from spec Section 2 point 1
    assert result.error is None, f"orchestrator errored: {result.error}"
    assert result.metrics["dbt_success"] is True
    assert result.metrics["row_count_match"] is True
    assert result.metrics["target_columns_covered"] == 1.0
    raw_expect, out_expect = staging_schemas_for_run(result.run_id)
    assert result.staging_raw_dataset == raw_expect
    assert result.staging_out_dataset == out_expect
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN (%s, %s)
                  AND table_name NOT LIKE %s ESCAPE %s
                """,
                (raw_expect, out_expect, "\\_dlt%", "\\"),
            )
            by_schema: dict[str, set[str]] = {raw_expect: set(), out_expect: set()}
            for sch, tbl in cur.fetchall():
                by_schema.setdefault(sch, set()).add(tbl)
    assert "contacts" in by_schema.get(raw_expect, set())
    assert "users" in by_schema.get(out_expect, set())
    assert "users" not in by_schema.get(raw_expect, set())
    assert "contacts" not in by_schema.get(out_expect, set())

    # Trace + artifacts on disk
    run_dir = tmp_path / "runs" / result.run_id
    assert (run_dir / "trace.jsonl").exists()
    assert (run_dir / "dbt_project" / "models" / "users.sql").exists()
    assert (run_dir / "strategy_result.json").exists()
    assert (run_dir / "mapping_manifest.json").exists()
    assert (run_dir / "result.json").exists()

    trace_text = (run_dir / "trace.jsonl").read_text()
    event_types = {json.loads(line)["event_type"] for line in trace_text.splitlines() if line.strip()}
    assert "strategy" in event_types
    assert "dbt_run" in event_types

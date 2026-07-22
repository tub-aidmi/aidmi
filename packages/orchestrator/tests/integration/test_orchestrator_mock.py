"""Integration test: full orchestrator end-to-end with MockStrategy."""

import asyncio
import json

import psycopg2

import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401
import aidmi_orchestrator.strategy  # noqa: F401
from aidmi_orchestrator.benchmark import Benchmark
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.scripts.init_fixtures import init_fixture
from aidmi_orchestrator.strategy.base import make_strategy
from aidmi_pipeline.config import out_schema_for_run


def test_mock_strategy_full_pipeline(staging_db_url, tmp_path):
    fixture = get_fixture("mock")
    init_fixture("mock", staging_db_url)

    mapping_source = str(fixture.target_schema_path.parent / "mock_mapping.json")
    strategy = make_strategy("mock", {"mapping_source": mapping_source})

    bench = Benchmark(fixture, workspace=tmp_path, staging_db_url=staging_db_url)
    result = asyncio.run(bench.run(strategy, strategy_spec_name="mock"))

    assert result.error is None, f"orchestrator errored: {result.error}"
    assert result.metrics["dbt_success"] is True
    assert result.metrics["row_count_match"] is True
    assert result.metrics["target_columns_covered"] == 1.0
    out_expect = out_schema_for_run(result.run_id)
    assert result.source_schema == fixture.source_schema
    assert result.out_schema == out_expect
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN (%s, %s)
                  AND table_name NOT LIKE %s ESCAPE %s
                """,
                (fixture.source_schema, out_expect, "\\_dlt%", "\\"),
            )
            by_schema: dict[str, set[str]] = {
                fixture.source_schema: set(),
                out_expect: set(),
            }
            for sch, tbl in cur.fetchall():
                by_schema.setdefault(sch, set()).add(tbl)
    assert "contacts" in by_schema.get(fixture.source_schema, set())
    assert "users" in by_schema.get(out_expect, set())
    assert "users" not in by_schema.get(fixture.source_schema, set())

    run_dir = tmp_path / "runs" / result.run_id
    assert (run_dir / "trace.jsonl").exists()
    assert (run_dir / "dbt_project" / "models" / "users.sql").exists()
    assert (run_dir / "strategy_result.json").exists()
    assert (run_dir / "mapping_manifest.json").exists()
    assert (run_dir / "result.json").exists()

    trace_text = (run_dir / "trace.jsonl").read_text()
    event_types = {
        json.loads(line)["event_type"]
        for line in trace_text.splitlines()
        if line.strip()
    }
    assert "strategy" in event_types
    assert "dbt_run" in event_types

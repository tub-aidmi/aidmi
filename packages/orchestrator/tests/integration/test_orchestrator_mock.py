"""Integration test: full orchestrator end-to-end with MockStrategy.

Asserts the success criteria from spec Section 2 point 1.
"""
import json
import asyncio
from pathlib import Path
import pytest

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

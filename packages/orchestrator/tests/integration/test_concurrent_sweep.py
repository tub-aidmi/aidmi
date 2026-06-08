"""Concurrent sweep over the mock strategy: results land, resume skips."""
from __future__ import annotations

import asyncio
import json

# Trigger registrations
import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import Benchmark
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.scheduler import (
    SweepJob, completed_keys, expand_jobs, filter_resumed, run_jobs,
)
from aidmi_orchestrator.strategy.base import make_strategy


def test_concurrent_mock_sweep_and_resume(staging_db_url, tmp_path):
    fixture = get_fixture("sp1_users")
    mapping_source = str(fixture.target_schema_path.parent / "mock_mapping.json")
    cells = [("mock", {"mapping_source": mapping_source}, "mock", None)]
    jobs = expand_jobs(cells, fixtures=["sp1_users"], runs_per_cell=2)
    assert len(jobs) == 2

    bench = Benchmark(fixture, workspace=tmp_path, staging_db_url=staging_db_url)
    results_path = tmp_path / "results.jsonl"
    lock = asyncio.Lock()

    async def run_job(job: SweepJob):
        strategy = make_strategy(job.registry_strategy, job.config)
        result = await bench.run(
            strategy, strategy_spec_name=job.spec_name, rep_index=job.rep_index,
        )
        async with lock:
            with open(results_path, "a", encoding="utf-8") as fh:
                fh.write(result.model_dump_json() + "\n")
        return result

    results = asyncio.run(run_jobs(jobs, run_job, concurrency=2))
    assert len(results) == 2
    assert all(r.error is None for r in results)

    rows = [json.loads(l) for l in results_path.read_text().splitlines()]
    assert {r["rep_index"] for r in rows} == {0, 1}
    assert all(r["metrics"]["dbt_success"] for r in rows)

    remaining = filter_resumed(jobs, completed_keys(results_path))
    assert remaining == []

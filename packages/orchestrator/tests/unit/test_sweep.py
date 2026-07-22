import asyncio
import re
from datetime import datetime

import pytest

from aidmi_orchestrator.campaign import Campaign
from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
from aidmi_orchestrator.scheduler import run_jobs as real_run_jobs
from aidmi_orchestrator.sweep import SweepSettings, run_sweep


def test_from_grid_reads_defaults():
    grid = {
        "fixture": ["master_v2", "messy_data_v2"],
        "runs_per_cell": 3,
        "concurrency": 4,
    }
    s = SweepSettings.from_grid(grid)
    assert s.fixtures == ["master_v2", "messy_data_v2"]
    assert s.runs_per_cell == 3
    assert s.concurrency == 4
    assert s.per_model_exclusive is False
    assert s.exclusive_prefixes == ("ise-",)


def test_scalar_fixture_key_becomes_a_list():
    assert SweepSettings.from_grid({"fixture": "master_v2"}).fixtures == ["master_v2"]


def test_fixture_override_wins():
    grid = {"fixture": ["master_v2", "messy_data_v2"]}
    assert SweepSettings.from_grid(grid, fixture_override="mock").fixtures == ["mock"]


def test_runs_per_cell_cli_override_only_when_not_one():
    grid = {"fixture": "mock", "runs_per_cell": 3}
    assert SweepSettings.from_grid(grid, runs_per_cell=1).runs_per_cell == 3
    assert SweepSettings.from_grid(grid, runs_per_cell=5).runs_per_cell == 5


def test_concurrency_defaults_to_three_then_grid_then_cli():
    assert SweepSettings.from_grid({"fixture": "mock"}).concurrency == 3
    assert (
        SweepSettings.from_grid({"fixture": "mock", "concurrency": 6}).concurrency == 6
    )
    assert (
        SweepSettings.from_grid(
            {"fixture": "mock", "concurrency": 6}, concurrency=2
        ).concurrency
        == 2
    )


def test_no_fixture_raises():
    with pytest.raises(ValueError, match="fixture"):
        SweepSettings.from_grid({})


def test_exclusive_prefixes_from_grid():
    s = SweepSettings.from_grid(
        {"fixture": "mock", "exclusive_model_prefixes": ["ise-", "vllm-"]}
    )
    assert s.exclusive_prefixes == ("ise-", "vllm-")


class FakeBench:
    def __init__(self, fixture_name: str, seen: list):
        self.fixture_name = fixture_name
        self.seen = seen

    async def run(
        self, strategy, *, strategy_spec_name, rep_index=0, trace_mirror=None
    ):
        self.seen.append((strategy_spec_name, self.fixture_name, rep_index))
        now = datetime(2026, 1, 1)
        return BenchmarkResult(
            run_id=f"r{len(self.seen)}",
            fixture_name=self.fixture_name,
            strategy_name="mock",
            strategy_spec_name=strategy_spec_name,
            strategy_config={},
            rep_index=rep_index,
            started_at=now,
            completed_at=now,
            wall_clock_seconds=0.0,
            strategy_result=StrategyResult(
                target_tables_written=["t"], self_reported_status="complete"
            ),
            metrics={"dbt_success": True},
        )


def _campaign(tmp_path):
    camp = Campaign(tmp_path / "camp")
    camp.ensure_layout()
    return camp


def test_run_sweep_runs_every_cell_fixture_rep(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run",
        lambda *a, **k: tmp_path / "bundle",
    )
    seen: list = []
    camp = _campaign(tmp_path)
    settings = SweepSettings.from_grid({"fixture": ["mock"], "runs_per_cell": 2})
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    results = asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    assert len(results) == 4
    assert sorted(seen) == [
        ("cell_a", "mock", 0),
        ("cell_a", "mock", 1),
        ("cell_b", "mock", 0),
        ("cell_b", "mock", 1),
    ]


def test_run_sweep_resume_skips_completed(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run", lambda *a, **k: tmp_path / "b"
    )
    seen: list = []
    camp = _campaign(tmp_path)
    camp.results_jsonl.write_text(
        '{"strategy_spec_name": "cell_a", "fixture_name": "mock", "rep_index": 0}\n',
        encoding="utf-8",
    )
    settings = SweepSettings.from_grid({"fixture": ["mock"]})
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    assert seen == [("cell_b", "mock", 0)]


def test_run_sweep_logs_progress_counters(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run",
        lambda *a, **k: tmp_path / "bundle",
    )
    seen: list = []
    camp = _campaign(tmp_path)
    settings = SweepSettings.from_grid({"fixture": ["mock"], "runs_per_cell": 2})
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    err = capsys.readouterr().err
    starting = re.findall(r"\[(\d+)/(\d+)\] starting", err)
    finished = re.findall(r"\[(\d+)/(\d+)\] finished", err)

    assert len(starting) == 4
    assert len(finished) == 4
    assert {int(pos) for pos, _ in starting} == {1, 2, 3, 4}
    assert {int(pos) for pos, _ in finished} == {1, 2, 3, 4}
    assert {total for _, total in starting} == {"4"}
    assert {total for _, total in finished} == {"4"}


@pytest.mark.parametrize("archive_dbt", [True, False])
def test_run_sweep_record_run_receives_cell_spec_and_archive_dbt(
    tmp_path, monkeypatch, archive_dbt
):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    config = {"foo": "bar"}
    cells = [("mock_strategy", config, "cell_a", None)]
    calls: list = []

    def fake_record_run(
        campaign_dir,
        result,
        workspace_run_dir,
        *,
        strategy_spec_path=None,
        cell_spec=None,
        archive_dbt=True,
    ):
        calls.append({"cell_spec": cell_spec, "archive_dbt": archive_dbt})
        return tmp_path / "bundle"

    monkeypatch.setattr("aidmi_orchestrator.sweep.record_run", fake_record_run)
    seen: list = []
    camp = _campaign(tmp_path)
    settings = SweepSettings.from_grid({"fixture": ["mock"]})

    asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
            archive_dbt=archive_dbt,
        )
    )

    assert len(calls) == 1
    assert calls[0]["cell_spec"] == {
        "name": "cell_a",
        "strategy": "mock_strategy",
        "config": config,
    }
    assert calls[0]["archive_dbt"] is archive_dbt


def test_run_sweep_record_run_executes_while_lock_held(tmp_path, monkeypatch):
    """A regression that moves record_run out of `async with lock` must fail this."""
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )

    captured_locks: list = []
    real_lock_cls = asyncio.Lock

    def tracking_lock(*args, **kwargs):
        lock = real_lock_cls(*args, **kwargs)
        captured_locks.append(lock)
        return lock

    monkeypatch.setattr("aidmi_orchestrator.sweep.asyncio.Lock", tracking_lock)

    observed_locked: list = []

    def fake_record_run(*args, **kwargs):
        observed_locked.append(captured_locks[0].locked())
        return tmp_path / "bundle"

    monkeypatch.setattr("aidmi_orchestrator.sweep.record_run", fake_record_run)

    seen: list = []
    camp = _campaign(tmp_path)
    settings = SweepSettings.from_grid(
        {"fixture": ["mock"], "runs_per_cell": 2, "concurrency": 4}
    )
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    assert len(observed_locked) == 4
    assert all(observed_locked), "record_run ran without the sweep lock held"


def test_run_sweep_no_resume_clears_existing_results_and_runs_all(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run",
        lambda *a, **k: tmp_path / "bundle",
    )
    seen: list = []
    camp = _campaign(tmp_path)
    camp.results_jsonl.write_text(
        '{"strategy_spec_name": "cell_a", "fixture_name": "mock", "rep_index": 0}\n',
        encoding="utf-8",
    )
    settings = SweepSettings.from_grid({"fixture": ["mock"]})
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    results = asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
            resume=False,
        )
    )

    assert not camp.results_jsonl.exists()
    assert len(results) == 2
    assert sorted(seen) == [("cell_a", "mock", 0), ("cell_b", "mock", 0)]


def test_run_sweep_nothing_to_run_returns_empty_and_skips_bench(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run",
        lambda *a, **k: tmp_path / "bundle",
    )
    seen: list = []
    camp = _campaign(tmp_path)
    camp.results_jsonl.write_text(
        '{"strategy_spec_name": "cell_a", "fixture_name": "mock", "rep_index": 0}\n'
        '{"strategy_spec_name": "cell_b", "fixture_name": "mock", "rep_index": 0}\n',
        encoding="utf-8",
    )
    settings = SweepSettings.from_grid({"fixture": ["mock"]})
    cells = [("mock", {}, "cell_a", None), ("mock", {}, "cell_b", None)]

    results = asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    assert results == []
    assert seen == []


def test_run_sweep_passes_settings_concurrency_and_exclusivity_to_run_jobs(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.make_strategy", lambda name, cfg: object()
    )
    monkeypatch.setattr(
        "aidmi_orchestrator.sweep.record_run",
        lambda *a, **k: tmp_path / "bundle",
    )
    captured: dict = {}

    async def spy_run_jobs(
        jobs, run_job, *, concurrency, prefixes, per_model_exclusive
    ):
        captured["concurrency"] = concurrency
        captured["prefixes"] = prefixes
        captured["per_model_exclusive"] = per_model_exclusive
        return await real_run_jobs(
            jobs,
            run_job,
            concurrency=concurrency,
            prefixes=prefixes,
            per_model_exclusive=per_model_exclusive,
        )

    monkeypatch.setattr("aidmi_orchestrator.sweep.run_jobs", spy_run_jobs)

    seen: list = []
    camp = _campaign(tmp_path)
    settings = SweepSettings.from_grid(
        {
            "fixture": ["mock"],
            "concurrency": 5,
            "exclusive_model_prefixes": ["ise-", "vllm-"],
            "per_model_exclusive": True,
        }
    )
    cells = [("mock", {}, "cell_a", None)]

    asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: FakeBench(fx, seen),
            workspace=tmp_path / "ws",
        )
    )

    assert captured == {
        "concurrency": 5,
        "prefixes": ("ise-", "vllm-"),
        "per_model_exclusive": True,
    }
    assert seen == [("cell_a", "mock", 0)]

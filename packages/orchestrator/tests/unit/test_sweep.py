import asyncio
from datetime import datetime

import pytest

from aidmi_orchestrator.campaign import Campaign
from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
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

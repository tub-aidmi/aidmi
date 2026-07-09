import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.distribution import (
    fig_dist_by_fixture,
    fig_dist_by_fixture_for_strategy,
    fig_dist_by_strategy,
    fig_dist_by_strategy_for_fixture,
)

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_dist_by_strategy_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_dist_by_strategy(recs, tmp_path)
    assert out.name == "dist_by_strategy.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_dist_by_fixture_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_dist_by_fixture(recs, tmp_path)
    assert out.name == "dist_by_fixture.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_dist_by_strategy_for_fixture_writes_svg(tmp_path):
    recs = load_records([FIX])
    fixture = recs[0].fixture
    out = fig_dist_by_strategy_for_fixture(recs, tmp_path, fixture)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0
    assert out.name.startswith("dist_strategy__")


def test_dist_by_fixture_for_strategy_writes_svg(tmp_path):
    recs = load_records([FIX])
    cell = recs[0].cell
    out = fig_dist_by_fixture_for_strategy(recs, tmp_path, cell)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0
    assert out.name.startswith("dist_fixture__")


def test_dist_handles_all_sc_off(tmp_path):
    # with no self-correction-on runs the figure must still render (empty boxes)
    recs = [r for r in load_records([FIX]) if r.sc is not True]
    out = fig_dist_by_strategy(recs, tmp_path)
    assert out.exists() and out.stat().st_size > 0

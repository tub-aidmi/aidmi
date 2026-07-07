import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.strategy import fig_cost_latency, fig_scorecard

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_scorecard_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_scorecard(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_cost_latency_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_cost_latency(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

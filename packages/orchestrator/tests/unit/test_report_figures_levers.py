import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.context import fig_ctx_comparison
from aidmi_orchestrator.report.figures.levers import fig_lever_ctx, fig_lever_sc

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_lever_sc_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_lever_sc(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_lever_ctx_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_lever_ctx(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_ctx_comparison_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_ctx_comparison(recs, tmp_path)
    assert out.name == "ctx_comparison.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

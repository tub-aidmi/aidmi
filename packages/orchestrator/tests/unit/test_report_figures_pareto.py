import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.pareto import fig_pareto

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_pareto_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_pareto(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

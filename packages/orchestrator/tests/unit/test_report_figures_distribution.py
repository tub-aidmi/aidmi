import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.distribution import (
    fig_metric_distribution,
    fig_score_histogram,
)

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_metric_distribution_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_metric_distribution(recs, tmp_path)
    assert out.name == "metric_distribution.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_score_histogram_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_score_histogram(recs, tmp_path)
    assert out.name == "score_histogram.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

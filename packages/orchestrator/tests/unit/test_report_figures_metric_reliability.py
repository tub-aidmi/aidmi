import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.correlation import (
    fig_recall_field_acc,
    fig_tokens_vs_field_acc,
    fig_tokens_vs_mat_rate,
    fig_tokens_vs_recall,
)
from aidmi_orchestrator.report.figures.metric import fig_prec_recall
from aidmi_orchestrator.report.figures.reliability import fig_rep_range, fig_rep_spread

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_prec_recall_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_prec_recall(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_recall_field_acc_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_recall_field_acc(recs, tmp_path)
    assert out.name == "recall_field_acc.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_tokens_correlation_figures_write_svg(tmp_path):
    recs = load_records([FIX])
    for fn, name in (
        (fig_tokens_vs_recall, "corr_tokens_recall.svg"),
        (fig_tokens_vs_field_acc, "corr_tokens_field_acc.svg"),
        (fig_tokens_vs_mat_rate, "corr_tokens_mat_rate.svg"),
    ):
        out = fn(recs, tmp_path)
        assert out.name == name
        assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_rep_spread_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_rep_spread(recs, tmp_path)
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_rep_range_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_rep_range(recs, tmp_path)
    assert out.name == "rep_range.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

import matplotlib

matplotlib.use("Agg")
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.figures.heatmap import fig_heatmap, fig_std_heatmap

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"


def test_heatmap_materialized_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_heatmap(
        recs, tmp_path,
        metric="materialized", filename="heatmap_materialized.svg",
        title="Materialization rate by cell x fixture",
    )
    assert out.name == "heatmap_materialized.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_heatmap_field_acc_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_heatmap(
        recs, tmp_path,
        metric="field_acc", filename="heatmap_field_acc.svg",
        title="Field accuracy by cell x fixture",
    )
    assert out.name == "heatmap_field_acc.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0


def test_std_heatmap_writes_svg(tmp_path):
    recs = load_records([FIX])
    out = fig_std_heatmap(recs, tmp_path)
    assert out.name == "heatmap_f1_std.svg"
    assert out.exists() and out.suffix == ".svg" and out.stat().st_size > 0

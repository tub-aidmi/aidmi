from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.driver import build_report

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"

CORE_FIGURES = [
    "pareto.svg",
    "prec_recall.svg",
    "lever_sc.svg",
    "lever_ctx.svg",
    "scorecard.svg",
    "cost_latency.svg",
    "rep_spread.svg",
    "heatmap_materialized.svg",
    "heatmap_field_acc.svg",
]


def test_build_report_writes_gallery_and_core_figures(tmp_path):
    all_records = load_records([FIX])
    # the mini fixture spans two models; pin to one so this test exercises
    # the single-model (no Cross-campaign section) path deterministically.
    single_model = all_records[0].model
    records = [r for r in all_records if r.model == single_model]

    paths = build_report(records, tmp_path)

    index = tmp_path / "index.html"
    tidy = tmp_path / "tidy.csv"
    assert index.exists()
    assert tidy.exists()
    for name in CORE_FIGURES:
        fig = tmp_path / "figures" / name
        assert fig.exists(), f"missing {name}"

    html = index.read_text()
    assert "<nav" in html
    assert "Cross-campaign" not in html

    assert index in paths
    assert tidy in paths
    for name in CORE_FIGURES:
        assert (tmp_path / "figures" / name) in paths


def test_build_report_multi_model_adds_cross_campaign_section(tmp_path):
    records = load_records([FIX])
    other_model_records = [
        r.__class__(**{**r.__dict__, "model": "other-model"}) for r in records
    ]
    two_model_records = records + other_model_records

    paths = build_report(two_model_records, tmp_path)

    index = tmp_path / "index.html"
    html = index.read_text()
    assert "Cross-campaign" in html
    assert 'id="cross_campaign"' in html

    models = sorted({r.model for r in two_model_records})
    per_model_heatmaps = [
        tmp_path / "figures" / f"heatmap_materialized_{model}.svg" for model in models
    ]
    assert any(p.exists() for p in per_model_heatmaps)
    assert any(p in paths for p in per_model_heatmaps)

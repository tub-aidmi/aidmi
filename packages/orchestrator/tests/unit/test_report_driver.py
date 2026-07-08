from __future__ import annotations

import re
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.driver import build_report

NAV_ORDER_SINGLE_MODEL = [
    "summary", "metric_choice", "distribution", "levers", "strategy",
    "efficiency", "fixtures",
]
NAV_ORDER_MULTI_MODEL = [
    "summary", "metric_choice", "distribution", "levers", "strategy",
    "efficiency", "fixtures", "cross_campaign",
]

FIX = Path(__file__).parent / "fixtures" / "mini_results.jsonl"

CORE_FIGURES = [
    "pareto.svg",
    "prec_recall.svg",
    "metric_distribution.svg",
    "score_histogram.svg",
    "lever_sc.svg",
    "lever_ctx.svg",
    "ctx_comparison.svg",
    "scorecard.svg",
    "cost_latency.svg",
    "thinking_tokens.svg",
    "efficiency.svg",
    "cost_drivers.svg",
    "rep_spread.svg",
    "rep_range.svg",
    "heatmap_f1_std.svg",
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
    assert re.findall(r'href="#(\w+)"', html) == NAV_ORDER_SINGLE_MODEL

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
    assert re.findall(r'href="#(\w+)"', html) == NAV_ORDER_MULTI_MODEL

    models = sorted({r.model for r in two_model_records})
    # Every model must get its own materialization heatmap, on disk AND in the
    # returned paths -- any() would let a dropped model (filename collision,
    # off-by-one) slip through, and per-model heatmaps are the whole point here.
    for model in models:
        heatmap = tmp_path / "figures" / f"heatmap_materialized_{model}.svg"
        assert heatmap.exists(), f"missing per-model heatmap for {model}"
        assert heatmap in paths, f"per-model heatmap for {model} not returned"

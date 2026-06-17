from __future__ import annotations

import aidmi_orchestrator.report  # noqa: F401
from aidmi_orchestrator.report.base import PlotKind, PlotScope
from aidmi_orchestrator.report.catalog import build_report_plan


def test_build_report_plan_headline_metrics() -> None:
    plan = build_report_plan()
    assert "target_columns_covered" in plan.headline_metrics
    assert "dbt_success" in plan.headline_metrics
    assert "manifest_present" in plan.headline_metrics
    assert plan.descriptors["target_columns_covered"].headline is True


def test_build_report_plan_global_heatmap_recipes() -> None:
    plan = build_report_plan()
    global_heatmaps = [
        r for r in plan.plot_recipes
        if r.scope == PlotScope.GLOBAL and r.kind == PlotKind.HEATMAP
    ]
    metrics = {r.metric for r in global_heatmaps}
    assert "target_columns_covered" in metrics
    assert "wall_clock_seconds" in metrics
    assert "manifest_present" not in metrics

"""Build report plan from contributor descriptors."""
from __future__ import annotations

from dataclasses import dataclass

from aidmi_orchestrator.report.base import (
    MetricDescriptor,
    PlotKind,
    PlotRecipe,
    PlotScope,
    all_metric_descriptors,
)


@dataclass
class ReportPlan:
    headline_metrics: list[str]
    descriptors: dict[str, MetricDescriptor]
    plot_recipes: list[PlotRecipe]


def build_report_plan() -> ReportPlan:
    descriptors_list = all_metric_descriptors()
    descriptors = {d.key: d for d in descriptors_list}

    headline_metrics: list[str] = []
    seen: set[str] = set()
    for d in descriptors_list:
        if d.headline and d.key not in seen:
            headline_metrics.append(d.key)
            seen.add(d.key)

    plot_recipes: list[PlotRecipe] = []
    for d in descriptors_list:
        if PlotScope.GLOBAL in d.plot_scopes:
            plot_recipes.append(PlotRecipe(
                scope=PlotScope.GLOBAL,
                kind=PlotKind.HEATMAP,
                metric=d.key,
            ))

    return ReportPlan(
        headline_metrics=headline_metrics,
        descriptors=descriptors,
        plot_recipes=plot_recipes,
    )

"""Plot artifact dispatch: SVG + companion CSV per plot."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from aidmi_orchestrator.report.aggregate import CellAggregate, RepSeries
from aidmi_orchestrator.report.catalog import ReportPlan
from aidmi_orchestrator.report.plot_specs import (
    DumbbellPlotSpec,
    FunnelPlotSpec,
    GroupedBarPlotSpec,
    HeatmapPlotSpec,
    PlotSpec,
    StrategyDistributionPlotSpec,
    TableModelHeatmapPlotSpec,
)
from aidmi_orchestrator.report.render.heatmap import build_global_heatmap_specs, render_plot
from aidmi_orchestrator.report.render.plot_csv import write_plot_csv
from aidmi_orchestrator.report.role_aggregate import RoleStackedBarSpec, build_all_role_stacked_bar_specs
from aidmi_orchestrator.report.strategy_plots import (
    build_funnel_specs,
    build_grouped_bar_specs,
    build_rep_stability_specs,
    build_row_equality_heatmap_specs,
    build_self_correction_specs,
)


def _artifact_base(out_dir: Path, spec: PlotSpec) -> Path:
    if isinstance(spec, HeatmapPlotSpec):
        return out_dir / spec.fixture / "global" / spec.metric
    if isinstance(spec, RoleStackedBarSpec):
        return out_dir / spec.fixture / "by_strategy" / spec.strategy / spec.metric
    if isinstance(spec, StrategyDistributionPlotSpec):
        return out_dir / spec.fixture / "by_strategy" / spec.strategy / f"rep_stability_{spec.metric}"
    if isinstance(spec, FunnelPlotSpec):
        return out_dir / spec.fixture / "by_strategy" / spec.strategy / "outcome_funnel"
    if isinstance(spec, GroupedBarPlotSpec):
        return out_dir / spec.fixture / "by_strategy" / spec.strategy / spec.plot_id
    if isinstance(spec, DumbbellPlotSpec):
        return out_dir / spec.fixture / "pairs" / "self_correction" / spec.metric
    if isinstance(spec, TableModelHeatmapPlotSpec):
        return out_dir / spec.fixture / "by_strategy" / spec.strategy / "row_equality_per_table"
    raise TypeError(f"unsupported plot spec type: {type(spec)!r}")


def render_plot_artifact(spec: PlotSpec, base_path: Path) -> list[Path]:
    svg_path = base_path.with_suffix(".svg")
    csv_path = base_path.with_suffix(".csv")
    render_plot(spec, svg_path)
    write_plot_csv(spec, csv_path)
    return [svg_path, csv_path]


def write_plots(
    cells: list[CellAggregate],
    series: list[RepSeries],
    rows: list[dict[str, Any]],
    out_dir: Path,
    plan: ReportPlan,
) -> list[Path]:
    del series
    written: list[Path] = []
    all_specs: list[PlotSpec] = []
    all_specs.extend(build_global_heatmap_specs(cells, plan))
    all_specs.extend(build_all_role_stacked_bar_specs(rows))
    all_specs.extend(build_rep_stability_specs(rows))
    all_specs.extend(build_funnel_specs(rows))
    all_specs.extend(build_grouped_bar_specs(rows))
    all_specs.extend(build_self_correction_specs(rows))
    all_specs.extend(build_row_equality_heatmap_specs(rows))
    for spec in all_specs:
        written.extend(render_plot_artifact(spec, _artifact_base(out_dir, spec)))
    return written

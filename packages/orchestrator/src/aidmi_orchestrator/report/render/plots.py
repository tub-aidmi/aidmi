"""Plot artifact dispatch: SVG + companion CSV per plot."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.aggregate import CellAggregate, RepSeries
from aidmi_orchestrator.report.catalog import ReportPlan
from aidmi_orchestrator.report.render.heatmap import (
    HeatmapPlotSpec,
    PlotSpec,
    build_global_heatmap_specs,
    render_plot,
)
from aidmi_orchestrator.report.render.plot_csv import write_plot_csv


def _artifact_base(out_dir: Path, spec: HeatmapPlotSpec) -> Path:
    return out_dir / spec.fixture / "global" / spec.metric


def render_plot_artifact(spec: PlotSpec, base_path: Path) -> list[Path]:
    svg_path = base_path.with_suffix(".svg")
    csv_path = base_path.with_suffix(".csv")
    render_plot(spec, svg_path)
    write_plot_csv(spec, csv_path)
    return [svg_path, csv_path]


def write_plots(
    cells: list[CellAggregate],
    series: list[RepSeries],
    out_dir: Path,
    plan: ReportPlan,
) -> list[Path]:
    del series  # reserved for future distribution plots
    written: list[Path] = []
    for spec in build_global_heatmap_specs(cells, plan):
        written.extend(render_plot_artifact(spec, _artifact_base(out_dir, spec)))
    return written

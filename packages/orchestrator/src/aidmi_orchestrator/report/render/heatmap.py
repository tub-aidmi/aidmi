"""Heatmap plot spec and SVG rendering."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Union

import numpy as np

from aidmi_orchestrator.report.base import MetricDescriptor, PlotKind, PlotScope
from aidmi_orchestrator.report.aggregate import CellAggregate
from aidmi_orchestrator.report.catalog import ReportPlan
from aidmi_orchestrator.report.layout.benchmark_grid import build_strategy_model_matrix


@dataclass
class HeatmapPlotSpec:
    fixture: str
    metric: str
    row_labels: list[str]
    col_labels: list[str]
    values: np.ndarray
    descriptor: MetricDescriptor


@dataclass
class DistributionPlotSpec:
    fixture: str
    metric: str
    group_labels: list[str]
    values_by_group: list[list[float]]
    descriptor: MetricDescriptor


PlotSpec = Union[HeatmapPlotSpec, DistributionPlotSpec]


def _cmap_for_descriptor(descriptor: MetricDescriptor) -> tuple[str, float | None, float | None]:
    if descriptor.vmin is not None and descriptor.vmax is not None:
        name = "YlOrRd_r" if descriptor.lower_is_better else "YlGn"
        return name, descriptor.vmin, descriptor.vmax
    if descriptor.lower_is_better:
        return "YlOrRd_r", None, None
    if descriptor.kind == "rate":
        return "YlGn", 0.0, 1.0
    return "viridis", None, None


def build_global_heatmap_spec(
    cells: list[CellAggregate],
    fixture: str,
    metric: str,
    descriptor: MetricDescriptor,
) -> HeatmapPlotSpec | None:
    built = build_strategy_model_matrix(cells, fixture, metric)
    if built is None:
        return None
    matrix, row_labels, col_labels = built
    return HeatmapPlotSpec(
        fixture=fixture,
        metric=metric,
        row_labels=row_labels,
        col_labels=col_labels,
        values=matrix,
        descriptor=descriptor,
    )


def build_global_heatmap_specs(
    cells: list[CellAggregate],
    plan: ReportPlan,
) -> list[HeatmapPlotSpec]:
    recipe_metrics = {
        r.metric for r in plan.plot_recipes
        if r.scope == PlotScope.GLOBAL and r.kind == PlotKind.HEATMAP
    }
    specs: list[HeatmapPlotSpec] = []
    for fixture in sorted({c.fixture_name for c in cells}):
        for metric in sorted(recipe_metrics):
            descriptor = plan.descriptors.get(metric)
            if descriptor is None:
                continue
            spec = build_global_heatmap_spec(cells, fixture, metric, descriptor)
            if spec is not None:
                specs.append(spec)
    return specs


def render_heatmap_svg(spec: HeatmapPlotSpec, svg_path: Path) -> None:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    matrix = spec.values
    cmap_name, vmin, vmax = _cmap_for_descriptor(spec.descriptor)
    cmap = plt.get_cmap(cmap_name).copy()
    cmap.set_bad(color="#d9d9d9")

    finite = matrix[np.isfinite(matrix)]
    if finite.size == 0:
        return
    if vmin is None:
        vmin = float(np.nanmin(matrix))
    if vmax is None:
        vmax = float(np.nanmax(matrix))
    if vmin == vmax:
        vmax = vmin + 1.0

    fig, ax = plt.subplots(
        figsize=(max(6.0, len(spec.col_labels) * 1.4), max(3.5, len(spec.row_labels) * 0.55)),
    )
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)

    ax.set_xticks(range(len(spec.col_labels)))
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.tick_params(axis="x", bottom=False, labelbottom=False, top=True, labeltop=True)
    ax.set_xlabel("Model", labelpad=10)
    ax.xaxis.set_label_position("top")

    ax.set_yticks(range(len(spec.row_labels)))
    ax.set_yticklabels(spec.row_labels)
    ax.set_ylabel("Strategy")
    ax.set_title(f"Mean {spec.metric} — {spec.fixture}", pad=28)

    rate_style = spec.descriptor.vmin is not None and spec.descriptor.vmax == 1.0
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            value = matrix[i, j]
            if not np.isfinite(value):
                label, text_color = "n/a", "black"
            elif rate_style:
                label = f"{value:.2f}"
                text_color = "white" if value >= 0.45 else "black"
            elif value >= 1000:
                label = f"{value:.3g}"
                text_color = "white" if value > (vmin + vmax) / 2 else "black"
            elif abs(value) < 10 and spec.metric not in (
                "tokens_input_total", "tokens_output_total", "wall_clock_seconds", "llm_calls_total",
            ):
                label = f"{value:.2f}"
                text_color = "white" if value >= (vmin + vmax) / 2 else "black"
            else:
                label = f"{value:.3g}"
                text_color = "white" if value > (vmin + vmax) / 2 else "black"
            ax.text(j, i, label, ha="center", va="center", color=text_color, fontsize=10)

    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
    cbar.set_label(spec.metric)
    fig.tight_layout()
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight")
    plt.close(fig)


def render_plot(spec: PlotSpec, svg_path: Path) -> None:
    if isinstance(spec, HeatmapPlotSpec):
        render_heatmap_svg(spec, svg_path)
    else:
        raise NotImplementedError(f"plot kind not implemented: {type(spec)!r}")

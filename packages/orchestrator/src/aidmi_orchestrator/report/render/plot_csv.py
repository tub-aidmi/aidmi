"""Plot companion CSV writers."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from aidmi_orchestrator.report.render.heatmap import DistributionPlotSpec, HeatmapPlotSpec, PlotSpec


def write_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def heatmap_csv_rows(spec: HeatmapPlotSpec) -> list[dict]:
    rows: list[dict] = []
    for i, strategy in enumerate(spec.row_labels):
        for j, model in enumerate(spec.col_labels):
            value = spec.values[i, j]
            rows.append({
                "strategy": strategy,
                "model": model,
                "value": "" if not np.isfinite(value) else value,
            })
    return rows


def distribution_csv_rows(spec: DistributionPlotSpec) -> list[dict]:
    rows: list[dict] = []
    for group, values in zip(spec.group_labels, spec.values_by_group):
        for rep_index, value in enumerate(values):
            rows.append({"group": group, "rep_index": rep_index, "value": value})
    return rows


def write_plot_csv(spec: PlotSpec, csv_path: Path) -> None:
    from aidmi_orchestrator.report.render.heatmap import DistributionPlotSpec, HeatmapPlotSpec

    if isinstance(spec, HeatmapPlotSpec):
        write_rows(csv_path, ["strategy", "model", "value"], heatmap_csv_rows(spec))
    elif isinstance(spec, DistributionPlotSpec):
        write_rows(csv_path, ["group", "rep_index", "value"], distribution_csv_rows(spec))
    else:
        raise TypeError(f"unsupported plot spec type: {type(spec)!r}")

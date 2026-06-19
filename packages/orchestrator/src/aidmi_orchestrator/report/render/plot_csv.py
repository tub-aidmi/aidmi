"""Plot companion CSV writers."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from aidmi_orchestrator.report.plot_specs import PlotSpec


def write_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def heatmap_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    for i, strategy in enumerate(spec.row_labels):
        for j, model in enumerate(spec.col_labels):
            value = spec.values[i, j]
            cell_n = spec.n[i, j] if hasattr(spec, "n") else float("nan")
            cell_std = spec.std[i, j] if hasattr(spec, "std") else float("nan")
            n_int = int(cell_n) if np.isfinite(cell_n) else None
            std_out = ""
            if n_int and n_int > 1 and np.isfinite(cell_std):
                std_out = cell_std
            rows.append({
                "strategy": strategy,
                "model": model,
                "value": "" if not np.isfinite(value) else value,
                "std": std_out,
                "n": "" if n_int is None else n_int,
            })
    return rows


def strategy_distribution_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    for model, values in zip(spec.col_labels, spec.values_by_model):
        for rep_index, value in enumerate(values):
            rows.append({
                "strategy": spec.strategy,
                "model": model,
                "rep_index": rep_index,
                "value": value,
            })
    return rows


def funnel_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    for stage, rates in zip(spec.stage_labels, spec.pass_rates):
        for model, rate in zip(spec.col_labels, rates):
            rows.append({
                "strategy": spec.strategy,
                "model": model,
                "stage": stage,
                "pass_rate": rate,
            })
    return rows


def grouped_bar_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    n_by_model = getattr(spec, "n_by_model", [])
    for series, vals in zip(spec.series_labels, spec.values):
        for j, (model, value) in enumerate(zip(spec.col_labels, vals)):
            row: dict = {
                "strategy": spec.strategy,
                "model": model,
                "series": series,
                "value": value,
            }
            if n_by_model and j < len(n_by_model):
                row["n"] = n_by_model[j]
            rows.append(row)
    return rows


def dumbbell_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    n_by_model = getattr(spec, "n_by_model", [])
    for j, (model, base, variant) in enumerate(
        zip(spec.col_labels, spec.base_values, spec.variant_values)
    ):
        row: dict = {
            "fixture": spec.fixture,
            "model": model,
            "base_strategy": spec.base_label,
            "variant_strategy": spec.variant_label,
            "base_value": base,
            "variant_value": variant,
        }
        if n_by_model and j < len(n_by_model):
            row["n"] = n_by_model[j]
        rows.append(row)
    return rows


def table_heatmap_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    for i, table in enumerate(spec.row_labels):
        for j, model in enumerate(spec.col_labels):
            value = spec.values[i, j]
            cell_n = spec.n[i, j] if hasattr(spec, "n") else float("nan")
            cell_std = spec.std[i, j] if hasattr(spec, "std") else float("nan")
            n_int = int(cell_n) if np.isfinite(cell_n) else None
            std_out = ""
            if n_int and n_int > 1 and np.isfinite(cell_std):
                std_out = cell_std
            rows.append({
                "strategy": spec.strategy,
                "table": table,
                "model": model,
                "value": "" if not np.isfinite(value) else value,
                "std": std_out,
                "n": "" if n_int is None else n_int,
            })
    return rows


def role_stacked_bar_csv_rows(spec) -> list[dict]:
    rows: list[dict] = []
    for model, seg, total in zip(spec.col_labels, spec.segments, spec.totals):
        for role in spec.role_labels:
            value = seg.get(role)
            if value is None:
                continue
            rows.append({
                "strategy": spec.strategy,
                "model": model,
                "role": role,
                "value": value,
                "total": total,
            })
    return rows


def write_plot_csv(spec: PlotSpec, csv_path: Path) -> None:
    from aidmi_orchestrator.report.plot_specs import (
        DumbbellPlotSpec,
        FunnelPlotSpec,
        GroupedBarPlotSpec,
        HeatmapPlotSpec,
        StrategyDistributionPlotSpec,
        TableModelHeatmapPlotSpec,
    )
    from aidmi_orchestrator.report.role_aggregate import RoleStackedBarSpec

    if isinstance(spec, HeatmapPlotSpec):
        write_rows(csv_path, ["strategy", "model", "value", "std", "n"], heatmap_csv_rows(spec))
    elif isinstance(spec, StrategyDistributionPlotSpec):
        write_rows(
            csv_path, ["strategy", "model", "rep_index", "value"],
            strategy_distribution_csv_rows(spec),
        )
    elif isinstance(spec, FunnelPlotSpec):
        write_rows(csv_path, ["strategy", "model", "stage", "pass_rate"], funnel_csv_rows(spec))
    elif isinstance(spec, GroupedBarPlotSpec):
        write_rows(
            csv_path, ["strategy", "model", "series", "value", "n"],
            grouped_bar_csv_rows(spec),
        )
    elif isinstance(spec, DumbbellPlotSpec):
        write_rows(
            csv_path,
            [
                "fixture", "model", "base_strategy", "variant_strategy",
                "base_value", "variant_value", "n",
            ],
            dumbbell_csv_rows(spec),
        )
    elif isinstance(spec, TableModelHeatmapPlotSpec):
        write_rows(
            csv_path, ["strategy", "table", "model", "value", "std", "n"],
            table_heatmap_csv_rows(spec),
        )
    elif isinstance(spec, RoleStackedBarSpec):
        write_rows(
            csv_path,
            ["strategy", "model", "role", "value", "total"],
            role_stacked_bar_csv_rows(spec),
        )
    else:
        raise TypeError(f"unsupported plot spec type: {type(spec)!r}")

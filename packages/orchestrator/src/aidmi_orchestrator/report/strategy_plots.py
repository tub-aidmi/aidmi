"""Builders for per-strategy and companion report plots."""
from __future__ import annotations

import statistics
from collections.abc import Callable
from typing import Any

from aidmi_orchestrator.report.aggregate import model_of, numeric_metrics
from aidmi_orchestrator.report.labels import strategy_label
from aidmi_orchestrator.report.layout.benchmark_grid import MODEL_LABELS, ordered_models
from aidmi_orchestrator.report.plot_specs import (
    DumbbellPlotSpec,
    FunnelPlotSpec,
    GroupedBarPlotSpec,
    StrategyDistributionPlotSpec,
    TableModelHeatmapPlotSpec,
)

FUNNEL_TARGET_COLUMNS_MIN = 0.9
FUNNEL_PRESERVATION_ROW_RATIO_MIN = 0.95

REP_STABILITY_METRICS = (
    "dbt_success",
    "preservation_row_ratio_mean",
    "llm_calls_total",
    "tokens_input_total",
)

SELF_CORRECTION_PAIRS = (
    ("structured_per_table", "structured_per_table_sc"),
)

SELF_CORRECTION_METRICS = (
    "dbt_success",
    "preservation_row_ratio_mean",
    "target_columns_covered",
)


def strategies_in_fixture(rows: list[dict[str, Any]], fixture: str) -> list[str]:
    labels = {strategy_label(r) for r in rows if r["fixture_name"] == fixture}
    return sorted(labels)


def rows_for_strategy(
    rows: list[dict[str, Any]], fixture: str, label: str,
) -> list[dict[str, Any]]:
    return [
        r for r in rows
        if r["fixture_name"] == fixture and strategy_label(r) == label
    ]


def _model_col_labels(models: list[str]) -> list[str]:
    return [MODEL_LABELS.get(m, m) for m in models]


def _by_model(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(model_of(row), []).append(row)
    return out


def mean_scalar_by_model(
    rows: list[dict[str, Any]], metric_key: str,
) -> tuple[list[str], list[str], list[float]] | None:
    by_model = _by_model(rows)
    models = ordered_models(set(by_model))
    means: list[float] = []
    for model in models:
        values = [
            numeric_metrics(r)[metric_key]
            for r in by_model[model]
            if metric_key in numeric_metrics(r)
        ]
        if not values:
            return None
        means.append(statistics.mean(values))
    return models, _model_col_labels(models), means


def rep_values_by_model(
    rows: list[dict[str, Any]], metric_key: str,
) -> tuple[list[str], list[str], list[list[float]]] | None:
    by_model = _by_model(rows)
    models = ordered_models(set(by_model))
    all_values: list[list[float]] = []
    for model in models:
        values = [
            numeric_metrics(r)[metric_key]
            for r in by_model[model]
            if metric_key in numeric_metrics(r)
        ]
        if not values:
            return None
        all_values.append(values)
    return models, _model_col_labels(models), all_values


def _metric_present(rows: list[dict[str, Any]], metric_key: str) -> bool:
    return any(metric_key in numeric_metrics(r) for r in rows)


def _build_grouped_bar(
    rows: list[dict[str, Any]],
    fixture: str,
    strategy: str,
    plot_id: str,
    series_keys: list[str],
) -> GroupedBarPlotSpec | None:
    cell_rows = rows_for_strategy(rows, fixture, strategy)
    if not cell_rows:
        return None
    present = [k for k in series_keys if _metric_present(cell_rows, k)]
    if not present:
        return None
    by_model = _by_model(cell_rows)
    models = ordered_models(set(by_model))
    col_labels = _model_col_labels(models)
    values: list[list[float]] = []
    for key in present:
        row_vals: list[float] = []
        for model in models:
            vals = [
                numeric_metrics(r)[key]
                for r in by_model.get(model, [])
                if key in numeric_metrics(r)
            ]
            row_vals.append(statistics.mean(vals) if vals else 0.0)
        values.append(row_vals)
    n_by_model = [len(by_model.get(model, [])) for model in models]
    return GroupedBarPlotSpec(
        fixture=fixture,
        strategy=strategy,
        plot_id=plot_id,
        series_labels=present,
        col_labels=col_labels,
        values=values,
        n_by_model=n_by_model,
    )


def _funnel_stages(rows: list[dict[str, Any]]) -> list[tuple[str, Callable[[dict[str, Any]], bool | None]]]:
    stages: list[tuple[str, Callable[[dict[str, Any]], bool | None]]] = [
        ("ran_ok", lambda r: r.get("error") is None),
        ("dbt_success", lambda r: bool((r.get("metrics") or {}).get("dbt_success"))),
    ]
    if any("target_columns_covered" in (r.get("metrics") or {}) for r in rows):
        stages.append((
            "schema_ok",
            lambda r: (
                (m := (r.get("metrics") or {}).get("target_columns_covered")) is not None
                and float(m) >= FUNNEL_TARGET_COLUMNS_MIN
            ),
        ))
    if any("preservation_row_ratio_mean" in (r.get("metrics") or {}) for r in rows):
        stages.append((
            "preservation_ok",
            lambda r: (
                (m := (r.get("metrics") or {}).get("preservation_row_ratio_mean")) is not None
                and float(m) >= FUNNEL_PRESERVATION_ROW_RATIO_MIN
            ),
        ))
    if any("row_count_match" in (r.get("metrics") or {}) for r in rows):
        stages.append((
            "row_match",
            lambda r: bool((r.get("metrics") or {}).get("row_count_match")),
        ))
    return stages


def build_rep_stability_specs(rows: list[dict[str, Any]]) -> list[StrategyDistributionPlotSpec]:
    specs: list[StrategyDistributionPlotSpec] = []
    fixtures = sorted({r["fixture_name"] for r in rows})
    for fixture in fixtures:
        for strategy in strategies_in_fixture(rows, fixture):
            cell_rows = rows_for_strategy(rows, fixture, strategy)
            for metric in REP_STABILITY_METRICS:
                built = rep_values_by_model(cell_rows, metric)
                if built is None:
                    continue
                _models, col_labels, values_by_model = built
                if not any(len(v) >= 2 for v in values_by_model):
                    continue
                specs.append(StrategyDistributionPlotSpec(
                    fixture=fixture,
                    strategy=strategy,
                    metric=metric,
                    col_labels=col_labels,
                    values_by_model=values_by_model,
                ))
    return specs


def build_funnel_specs(rows: list[dict[str, Any]]) -> list[FunnelPlotSpec]:
    specs: list[FunnelPlotSpec] = []
    fixtures = sorted({r["fixture_name"] for r in rows})
    for fixture in fixtures:
        for strategy in strategies_in_fixture(rows, fixture):
            cell_rows = rows_for_strategy(rows, fixture, strategy)
            stages = _funnel_stages(cell_rows)
            by_model = _by_model(cell_rows)
            models = ordered_models(set(by_model))
            if not models:
                continue
            col_labels = _model_col_labels(models)
            stage_labels = [s[0] for s in stages]
            pass_rates: list[list[float]] = []
            for _name, predicate in stages:
                rates: list[float] = []
                for model in models:
                    reps = by_model.get(model, [])
                    if not reps:
                        rates.append(0.0)
                        continue
                    passed = sum(1 for r in reps if predicate(r))
                    rates.append(passed / len(reps))
                pass_rates.append(rates)
            specs.append(FunnelPlotSpec(
                fixture=fixture,
                strategy=strategy,
                stage_labels=stage_labels,
                col_labels=col_labels,
                pass_rates=pass_rates,
                n_by_model=[len(by_model.get(model, [])) for model in models],
            ))
    return specs


def build_grouped_bar_specs(rows: list[dict[str, Any]]) -> list[GroupedBarPlotSpec]:
    specs: list[GroupedBarPlotSpec] = []
    fixtures = sorted({r["fixture_name"] for r in rows})
    for fixture in fixtures:
        for strategy in strategies_in_fixture(rows, fixture):
            for spec in (
                _build_grouped_bar(rows, fixture, strategy, "preservation_profile", [
                    "preservation_row_ratio_mean",
                    "preservation_distinct_ratio_mean",
                    "preservation_null_inflation_mean",
                ]),
                _build_grouped_bar(rows, fixture, strategy, "schema_errors", [
                    "type_mismatches", "extraneous_columns",
                ]),
                _build_grouped_bar(rows, fixture, strategy, "tokens_in_out", [
                    "tokens_input_total", "tokens_output_total",
                ]),
                _build_preservation_per_table_spec(rows, fixture, strategy),
            ):
                if spec is not None:
                    specs.append(spec)
    return specs


def _build_preservation_per_table_spec(
    rows: list[dict[str, Any]], fixture: str, strategy: str,
) -> GroupedBarPlotSpec | None:
    cell_rows = rows_for_strategy(rows, fixture, strategy)
    tables: set[str] = set()
    for row in cell_rows:
        pt = (row.get("metrics") or {}).get("preservation_per_table")
        if isinstance(pt, dict):
            tables.update(pt.keys())
    if not tables:
        return None
    table_labels = sorted(tables)
    by_model = _by_model(cell_rows)
    models = ordered_models(set(by_model))
    col_labels = _model_col_labels(models)
    values: list[list[float]] = []
    for table in table_labels:
        row_vals: list[float] = []
        for model in models:
            ratios = []
            for r in by_model.get(model, []):
                pt = (r.get("metrics") or {}).get("preservation_per_table") or {}
                entry = pt.get(table) if isinstance(pt, dict) else None
                if isinstance(entry, dict) and entry.get("row_ratio") is not None:
                    ratios.append(float(entry["row_ratio"]))
            row_vals.append(statistics.mean(ratios) if ratios else 0.0)
        values.append(row_vals)
    return GroupedBarPlotSpec(
        fixture=fixture,
        strategy=strategy,
        plot_id="preservation_per_table",
        series_labels=table_labels,
        col_labels=col_labels,
        values=values,
        n_by_model=[len(by_model.get(model, [])) for model in models],
    )


def build_row_equality_heatmap_specs(rows: list[dict[str, Any]]) -> list[TableModelHeatmapPlotSpec]:
    import numpy as np

    specs: list[TableModelHeatmapPlotSpec] = []
    fixtures = sorted({r["fixture_name"] for r in rows})
    for fixture in fixtures:
        for strategy in strategies_in_fixture(rows, fixture):
            cell_rows = rows_for_strategy(rows, fixture, strategy)
            tables: set[str] = set()
            for row in cell_rows:
                pt = (row.get("metrics") or {}).get("per_table_equality")
                if isinstance(pt, dict):
                    tables.update(pt.keys())
            if not tables:
                continue
            table_labels = sorted(tables)
            by_model = _by_model(cell_rows)
            models = ordered_models(set(by_model))
            col_labels = _model_col_labels(models)
            shape = (len(table_labels), len(models))
            matrix = np.full(shape, np.nan)
            std_matrix = np.full(shape, np.nan)
            n_matrix = np.full(shape, np.nan)
            for i, table in enumerate(table_labels):
                for j, model in enumerate(models):
                    vals = []
                    for r in by_model.get(model, []):
                        pt = (r.get("metrics") or {}).get("per_table_equality") or {}
                        entry = pt.get(table) if isinstance(pt, dict) else None
                        if isinstance(entry, dict) and "row_count_match" in entry:
                            vals.append(1.0 if entry["row_count_match"] else 0.0)
                    if vals:
                        matrix[i, j] = statistics.mean(vals)
                        n_matrix[i, j] = len(vals)
                        std_matrix[i, j] = statistics.stdev(vals) if len(vals) > 1 else 0.0
            specs.append(TableModelHeatmapPlotSpec(
                fixture=fixture,
                strategy=strategy,
                metric="row_count_match",
                row_labels=table_labels,
                col_labels=col_labels,
                values=matrix,
                std=std_matrix,
                n=n_matrix,
            ))
    return specs


def build_self_correction_specs(rows: list[dict[str, Any]]) -> list[DumbbellPlotSpec]:
    specs: list[DumbbellPlotSpec] = []
    fixtures = sorted({r["fixture_name"] for r in rows})
    for fixture in fixtures:
        for base_label, variant_label in SELF_CORRECTION_PAIRS:
            base_rows = rows_for_strategy(rows, fixture, base_label)
            variant_rows = rows_for_strategy(rows, fixture, variant_label)
            if not base_rows or not variant_rows:
                continue
            base_by_model = _by_model(base_rows)
            variant_by_model = _by_model(variant_rows)
            common_models = ordered_models(set(base_by_model) & set(variant_by_model))
            if not common_models:
                continue
            col_labels = _model_col_labels(common_models)
            for metric in SELF_CORRECTION_METRICS:
                base_vals: list[float] = []
                variant_vals: list[float] = []
                n_by_model: list[int] = []
                for model in common_models:
                    b = [
                        numeric_metrics(r)[metric]
                        for r in base_by_model[model]
                        if metric in numeric_metrics(r)
                    ]
                    v = [
                        numeric_metrics(r)[metric]
                        for r in variant_by_model[model]
                        if metric in numeric_metrics(r)
                    ]
                    if not b or not v:
                        break
                    base_vals.append(statistics.mean(b))
                    variant_vals.append(statistics.mean(v))
                    n_by_model.append(len(b))
                else:
                    specs.append(DumbbellPlotSpec(
                        fixture=fixture,
                        metric=metric,
                        base_label=base_label,
                        variant_label=variant_label,
                        col_labels=col_labels,
                        base_values=base_vals,
                        variant_values=variant_vals,
                        n_by_model=n_by_model,
                    ))
    return specs

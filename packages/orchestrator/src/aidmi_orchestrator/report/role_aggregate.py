"""Per-strategy role breakdown aggregation for stacked bar plots."""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any

from aidmi_orchestrator.report.aggregate import model_of
from aidmi_orchestrator.report.layout.benchmark_grid import MODEL_LABELS, ordered_models
from aidmi_orchestrator.report.labels import strategy_label

ROLE_ORDER = ("writer", "planner", "critic", "judge")

ROLE_STACKED_METRICS = (
    "tokens_input_by_role",
    "tokens_output_by_role",
    "llm_calls_by_role",
    "latency_ms_sum_by_role",
)


@dataclass
class RoleStackedBarSpec:
    fixture: str
    strategy: str
    metric: str
    col_labels: list[str]
    role_labels: list[str]
    segments: list[dict[str, float]]
    totals: list[float]
    n_by_model: list[int]


def _strategy_label(row: dict[str, Any]) -> str:
    return strategy_label(row)


def role_dict_metrics(row: dict[str, Any], metric_key: str) -> dict[str, float] | None:
    raw = (row.get("metrics") or {}).get(metric_key)
    if not isinstance(raw, dict) or not raw:
        return None
    return {str(k): float(v) for k, v in raw.items()}


def ordered_roles(roles: set[str]) -> list[str]:
    ordered = [r for r in ROLE_ORDER if r in roles]
    ordered.extend(sorted(roles - set(ordered)))
    return ordered


def multi_role_strategies(rows: list[dict[str, Any]]) -> list[str]:
    roles_by_label: dict[str, set[str]] = {}
    for row in rows:
        label = _strategy_label(row)
        for metric_key in ROLE_STACKED_METRICS:
            role_dict = role_dict_metrics(row, metric_key)
            if role_dict:
                roles_by_label.setdefault(label, set()).update(role_dict)
    return sorted(label for label, roles in roles_by_label.items() if len(roles) > 1)


def build_role_stacked_bar_spec(
    rows: list[dict[str, Any]],
    fixture: str,
    strategy_label: str,
    metric_key: str,
) -> RoleStackedBarSpec | None:
    cell_rows = [
        r for r in rows
        if r["fixture_name"] == fixture and _strategy_label(r) == strategy_label
    ]
    if not cell_rows:
        return None

    by_model: dict[str, list[dict[str, float]]] = {}
    all_roles: set[str] = set()
    for row in cell_rows:
        role_dict = role_dict_metrics(row, metric_key)
        if not role_dict:
            continue
        model = model_of(row)
        by_model.setdefault(model, []).append(role_dict)
        all_roles.update(role_dict)

    if not by_model or not all_roles:
        return None

    role_labels = ordered_roles(all_roles)
    col_models = ordered_models(set(by_model))
    col_labels = [MODEL_LABELS.get(m, m) for m in col_models]

    segments: list[dict[str, float]] = []
    totals: list[float] = []
    n_by_model: list[int] = []
    for model in col_models:
        dicts = by_model.get(model, [])
        seg: dict[str, float] = {}
        for role in role_labels:
            values = [d[role] for d in dicts if role in d]
            if values:
                seg[role] = statistics.mean(values)
        if seg:
            segments.append(seg)
            totals.append(sum(seg.values()))
            n_by_model.append(len(dicts))
        else:
            segments.append({})
            totals.append(0.0)
            n_by_model.append(0)

    return RoleStackedBarSpec(
        fixture=fixture,
        strategy=strategy_label,
        metric=metric_key,
        col_labels=col_labels,
        role_labels=role_labels,
        segments=segments,
        totals=totals,
        n_by_model=n_by_model,
    )


def build_all_role_stacked_bar_specs(rows: list[dict[str, Any]]) -> list[RoleStackedBarSpec]:
    fixtures = sorted({r["fixture_name"] for r in rows})
    strategies = multi_role_strategies(rows)
    specs: list[RoleStackedBarSpec] = []
    for fixture in fixtures:
        for strategy in strategies:
            for metric_key in ROLE_STACKED_METRICS:
                spec = build_role_stacked_bar_spec(rows, fixture, strategy, metric_key)
                if spec is not None:
                    specs.append(spec)
    return specs

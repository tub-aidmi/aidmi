"""Load and aggregate benchmark results.jsonl."""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aidmi_orchestrator.report.layout.benchmark_grid import heatmap_row_key


@dataclass
class CellAggregate:
    fixture_name: str
    spec_name: str
    strategy_name: str
    model_name: str
    n_runs: int
    metrics: dict[str, dict[str, float]]


@dataclass(frozen=True)
class RepSeries:
    fixture_name: str
    row_key: str
    model_name: str
    metric: str
    values: list[float]


def load_results(paths: list[Path]) -> list[dict[str, Any]]:
    from aidmi_orchestrator.campaign import results_jsonl_for_campaign

    rows: list[dict[str, Any]] = []
    for p in paths:
        if p.is_dir():
            campaign_jsonl = results_jsonl_for_campaign(p)
            target = campaign_jsonl if campaign_jsonl is not None else p / "results.jsonl"
        else:
            target = p
        if not target.is_file():
            continue
        for line in target.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def model_of(row: dict[str, Any]) -> str:
    cfg = row.get("strategy_config") or {}
    for key in ("writer_model", "planner_model"):
        value = cfg.get(key)
        if isinstance(value, dict) and value.get("model_name"):
            return value["model_name"]
    return "-"


def numeric_metrics(row: dict[str, Any]) -> dict[str, float]:
    out = {
        "ran_ok": 1.0 if row.get("error") is None else 0.0,
        "wall_clock_seconds": float(row.get("wall_clock_seconds", 0.0)),
    }
    for key, value in (row.get("metrics") or {}).items():
        if isinstance(value, bool):
            out[key] = 1.0 if value else 0.0
        elif isinstance(value, (int, float)):
            out[key] = float(value)
    return out


def aggregate(rows: list[dict[str, Any]]) -> list[CellAggregate]:
    by_cell: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        by_cell.setdefault((row["fixture_name"], row["strategy_spec_name"]), []).append(row)

    cells: list[CellAggregate] = []
    for (fixture, spec), cell_rows in sorted(by_cell.items()):
        numeric = [numeric_metrics(r) for r in cell_rows]
        metric_names = sorted({k for n in numeric for k in n})
        metrics: dict[str, dict[str, float]] = {}
        for name in metric_names:
            values = [n[name] for n in numeric if name in n]
            metrics[name] = {
                "mean": statistics.mean(values),
                "std": statistics.stdev(values) if len(values) > 1 else 0.0,
                "n": float(len(values)),
            }
        cells.append(CellAggregate(
            fixture_name=fixture,
            spec_name=spec,
            strategy_name=cell_rows[0].get("strategy_name", spec),
            model_name=model_of(cell_rows[0]),
            n_runs=len(cell_rows),
            metrics=metrics,
        ))
    return cells


def build_rep_series(rows: list[dict[str, Any]]) -> list[RepSeries]:
    buckets: dict[tuple[str, str, str, str], list[float]] = {}
    for row in rows:
        fixture = row["fixture_name"]
        spec = row["strategy_spec_name"]
        strategy = row.get("strategy_name", spec)
        row_key = heatmap_row_key(spec, strategy)
        model = model_of(row)
        nums = numeric_metrics(row)
        for metric, value in nums.items():
            buckets.setdefault((fixture, row_key, model, metric), []).append(value)

    return [
        RepSeries(fixture_name=k[0], row_key=k[1], model_name=k[2], metric=k[3], values=v)
        for k, v in sorted(buckets.items())
    ]

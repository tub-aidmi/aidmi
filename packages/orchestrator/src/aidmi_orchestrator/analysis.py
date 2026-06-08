"""Aggregate benchmark results.jsonl into report-ready tables."""
from __future__ import annotations

import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_HEADLINE_METRICS = [
    "ran_ok", "dbt_success", "target_columns_covered", "type_mismatches",
    "extraneous_columns", "row_count_match",
    "preservation_row_ratio_mean", "preservation_empty_tables",
    "preservation_null_inflation_mean", "preservation_distinct_ratio_mean",
    "manifest_present", "manifest_table_coverage", "manifest_column_coverage",
    "llm_calls_total", "tokens_input_total", "tokens_output_total",
    "dollar_cost_total", "wall_clock_seconds",
]


@dataclass
class CellAggregate:
    fixture_name: str
    spec_name: str
    strategy_name: str
    model_name: str
    n_runs: int
    metrics: dict[str, dict[str, float]]


def load_results(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in paths:
        target = p / "results.jsonl" if p.is_dir() else p
        for line in target.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _model_of(row: dict[str, Any]) -> str:
    cfg = row.get("strategy_config") or {}
    for key in ("writer_model", "planner_model"):
        value = cfg.get(key)
        if isinstance(value, dict) and value.get("model_name"):
            return value["model_name"]
    return "-"


def _numeric_metrics(row: dict[str, Any]) -> dict[str, float]:
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
        numeric = [_numeric_metrics(r) for r in cell_rows]
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
            model_name=_model_of(cell_rows[0]),
            n_runs=len(cell_rows),
            metrics=metrics,
        ))
    return cells


def _fmt(agg: dict[str, float] | None) -> str:
    if agg is None:
        return "-"
    if agg["std"]:
        return f"{agg['mean']:.3g}±{agg['std']:.2g}"
    return f"{agg['mean']:.4g}"


def render_markdown(
    cells: list[CellAggregate],
    headline_metrics: list[str] = DEFAULT_HEADLINE_METRICS,
) -> str:
    lines: list[str] = ["# Benchmark summary", ""]
    fixtures = sorted({c.fixture_name for c in cells})
    for fixture in fixtures:
        f_cells = [c for c in cells if c.fixture_name == fixture]
        present = [m for m in headline_metrics if any(m in c.metrics for c in f_cells)]
        lines.append(f"## {fixture}")
        lines.append("")
        lines.append("| spec | model | n | " + " | ".join(present) + " |")
        lines.append("|" + "---|" * (len(present) + 3))
        for c in sorted(f_cells, key=lambda c: c.spec_name):
            cols = [_fmt(c.metrics.get(m)) for m in present]
            lines.append(f"| {c.spec_name} | {c.model_name} | {c.n_runs} | " + " | ".join(cols) + " |")
        lines.append("")
    return "\n".join(lines)


def render_matrix(cells: list[CellAggregate], metric: str) -> str:
    lines: list[str] = [f"# Strategy × model — {metric} (mean)", ""]
    for fixture in sorted({c.fixture_name for c in cells}):
        f_cells = [c for c in cells if c.fixture_name == fixture and metric in c.metrics]
        if not f_cells:
            continue
        strategies = sorted({c.strategy_name for c in f_cells})
        models = sorted({c.model_name for c in f_cells})
        lines.append(f"## {fixture}")
        lines.append("")
        lines.append("| strategy | " + " | ".join(models) + " |")
        lines.append("|" + "---|" * (len(models) + 1))
        for strategy in strategies:
            row = [strategy]
            for model in models:
                matching = [
                    c.metrics[metric]["mean"]
                    for c in f_cells
                    if c.strategy_name == strategy and c.model_name == model
                ]
                row.append(f"{statistics.mean(matching):.3g}" if matching else "-")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    return "\n".join(lines)


def write_csvs(cells: list[CellAggregate], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "cells.csv", "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["fixture", "spec", "strategy", "model", "metric", "mean", "std", "n"])
        for c in cells:
            for metric, agg in sorted(c.metrics.items()):
                writer.writerow([
                    c.fixture_name, c.spec_name, c.strategy_name, c.model_name,
                    metric, agg["mean"], agg["std"], int(agg["n"]),
                ])
    metric_names = sorted({m for c in cells for m in c.metrics})
    with open(out_dir / "summary.csv", "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["fixture", "spec", "strategy", "model", "n_runs", *metric_names])
        for c in cells:
            writer.writerow([
                c.fixture_name, c.spec_name, c.strategy_name, c.model_name, c.n_runs,
                *[c.metrics[m]["mean"] if m in c.metrics else "" for m in metric_names],
            ])


def write_plots(
    cells: list[CellAggregate], out_dir: Path,
    metrics: list[str] = DEFAULT_HEADLINE_METRICS,
) -> list[Path]:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for fixture in sorted({c.fixture_name for c in cells}):
        f_cells = sorted(
            (c for c in cells if c.fixture_name == fixture), key=lambda c: c.spec_name,
        )
        for metric in metrics:
            with_metric = [c for c in f_cells if metric in c.metrics]
            if not with_metric:
                continue
            fig, ax = plt.subplots(figsize=(max(6, len(with_metric) * 0.6), 4))
            names = [c.spec_name for c in with_metric]
            means = [c.metrics[metric]["mean"] for c in with_metric]
            stds = [c.metrics[metric]["std"] for c in with_metric]
            ax.bar(range(len(names)), means, yerr=stds, capsize=3)
            ax.set_xticks(range(len(names)))
            ax.set_xticklabels(names, rotation=45, ha="right", fontsize=7)
            ax.set_title(f"{fixture}: {metric}")
            fig.tight_layout()
            path = out_dir / f"{fixture}_{metric}.png"
            fig.savefig(path, dpi=120)
            plt.close(fig)
            written.append(path)
    return written

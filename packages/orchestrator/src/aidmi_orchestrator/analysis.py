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

GLOBAL_HEATMAP_METRICS = [
    "target_columns_covered",
    "dbt_success",
    "type_mismatches",
    "extraneous_columns",
    "preservation_row_ratio_mean",
    "preservation_distinct_ratio_mean",
    "preservation_null_inflation_mean",
    "row_count_match",
    "ran_ok",
    "tokens_input_total",
    "tokens_output_total",
    "llm_calls_total",
    "wall_clock_seconds",
    "dollar_cost_total",
]

_ROW_SUFFIXES = ("_writer_model_", "_planner_model_")

_CELL_BASE_LABELS: dict[str, str] = {
    "critique": "write_then_critique",
    "freeform_sc": "write_tools_freeform",
    "structured": "structured_per_table",
    "structured_sc": "structured_per_table_sc",
    "plan": "plan_then_execute",
    "ensemble": "ensemble_vote",
}

_ROW_LABEL_ORDER = [
    "write_then_critique",
    "write_tools_freeform",
    "structured_per_table",
    "structured_per_table_sc",
    "plan_then_execute",
    "ensemble_vote",
]

_MODEL_ORDER = [
    "academic/qwen3.5-397b-a17b",
    "academic/devstral-2-123b-instruct-2512",
    "ise-ollama/qwen3.6:35b-a3b",
    "ise-openai-nvidia/qwen35-9b",
]

_MODEL_LABELS: dict[str, str] = {
    "academic/qwen3.5-397b-a17b": "qwen3.5-397b",
    "academic/devstral-2-123b-instruct-2512": "devstral-123b",
    "ise-ollama/qwen3.6:35b-a3b": "qwen3.6-35b",
    "ise-openai-nvidia/qwen35-9b": "qwen3.5-9b",
}

# metric -> (cmap_name, vmin, vmax, lower_is_better)
_METRIC_PLOT_STYLE: dict[str, tuple[str, float | None, float | None, bool]] = {
    "target_columns_covered": ("YlGn", 0.0, 1.0, False),
    "dbt_success": ("YlGn", 0.0, 1.0, False),
    "row_count_match": ("YlGn", 0.0, 1.0, False),
    "ran_ok": ("YlGn", 0.0, 1.0, False),
    "preservation_row_ratio_mean": ("YlGn", 0.0, 1.0, False),
    "preservation_distinct_ratio_mean": ("YlGn", 0.0, 1.0, False),
    "type_mismatches": ("YlOrRd_r", None, None, True),
    "extraneous_columns": ("YlOrRd_r", None, None, True),
    "preservation_null_inflation_mean": ("YlOrRd_r", None, None, True),
}


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


def heatmap_cell_base(spec_name: str) -> str | None:
    for suffix in _ROW_SUFFIXES:
        idx = spec_name.find(suffix)
        if idx != -1:
            return spec_name[:idx]
    return None


def heatmap_row_key(spec_name: str, strategy_name: str) -> str:
    base = heatmap_cell_base(spec_name)
    if base is not None:
        return base
    return strategy_name


def heatmap_row_label(row_key: str, strategy_name: str) -> str:
    return _CELL_BASE_LABELS.get(row_key, strategy_name)


def _ordered_row_labels(keys: set[str], cells_by_key: dict[str, str]) -> list[str]:
    labels = {heatmap_row_label(k, cells_by_key[k]) for k in keys}
    ordered = [label for label in _ROW_LABEL_ORDER if label in labels]
    ordered.extend(sorted(labels - set(ordered)))
    return ordered


def _ordered_models(models: set[str]) -> list[str]:
    ordered = [m for m in _MODEL_ORDER if m in models]
    ordered.extend(sorted(models - set(ordered)))
    return ordered


def build_strategy_model_matrix(
    cells: list[CellAggregate],
    fixture: str,
    metric: str,
) -> tuple[Any, list[str], list[str]] | None:
    import numpy as np

    f_cells = [c for c in cells if c.fixture_name == fixture and metric in c.metrics]
    if not f_cells:
        return None

    by_row_model: dict[tuple[str, str], float] = {}
    key_to_strategy: dict[str, str] = {}
    models: set[str] = set()
    row_keys: set[str] = set()
    for c in f_cells:
        row_key = heatmap_row_key(c.spec_name, c.strategy_name)
        row_keys.add(row_key)
        key_to_strategy.setdefault(row_key, c.strategy_name)
        models.add(c.model_name)
        by_row_model[(row_key, c.model_name)] = c.metrics[metric]["mean"]

    row_labels = _ordered_row_labels(row_keys, key_to_strategy)
    col_models = _ordered_models(models)
    key_by_label = {heatmap_row_label(k, key_to_strategy[k]): k for k in row_keys}

    matrix = np.full((len(row_labels), len(col_models)), np.nan)
    for i, label in enumerate(row_labels):
        row_key = key_by_label[label]
        for j, model in enumerate(col_models):
            value = by_row_model.get((row_key, model))
            if value is not None:
                matrix[i, j] = value

    col_labels = [_MODEL_LABELS.get(m, m) for m in col_models]
    return matrix, row_labels, col_labels


def write_global_heatmaps(
    cells: list[CellAggregate],
    out_dir: Path,
    metrics: list[str] = GLOBAL_HEATMAP_METRICS,
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
    import numpy as np

    written: list[Path] = []
    for fixture in sorted({c.fixture_name for c in cells}):
        plot_dir = out_dir / fixture / "global"
        plot_dir.mkdir(parents=True, exist_ok=True)
        for metric in metrics:
            built = build_strategy_model_matrix(cells, fixture, metric)
            if built is None:
                continue
            matrix, row_labels, col_labels = built

            style = _METRIC_PLOT_STYLE.get(metric, ("viridis", None, None, False))
            cmap_name, vmin, vmax, _lower_is_better = style
            cmap = plt.get_cmap(cmap_name).copy()
            cmap.set_bad(color="#d9d9d9")

            finite = matrix[np.isfinite(matrix)]
            if finite.size == 0:
                continue
            if vmin is None:
                vmin = float(np.nanmin(matrix))
            if vmax is None:
                vmax = float(np.nanmax(matrix))
            if vmin == vmax:
                vmax = vmin + 1.0

            fig, ax = plt.subplots(figsize=(max(6.0, len(col_labels) * 1.4), max(3.5, len(row_labels) * 0.55)))
            im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)

            ax.set_xticks(range(len(col_labels)))
            ax.set_xticklabels(col_labels, rotation=35, ha="right")
            ax.tick_params(axis="x", bottom=False, labelbottom=False, top=True, labeltop=True)
            ax.set_xlabel("Model", labelpad=10)
            ax.xaxis.set_label_position("top")

            ax.set_yticks(range(len(row_labels)))
            ax.set_yticklabels(row_labels)
            ax.set_ylabel("Strategy")
            ax.set_title(f"Mean {metric} — {fixture}", pad=28)

            for i in range(matrix.shape[0]):
                for j in range(matrix.shape[1]):
                    value = matrix[i, j]
                    if not np.isfinite(value):
                        label, text_color = "n/a", "black"
                    elif metric in _METRIC_PLOT_STYLE and _METRIC_PLOT_STYLE[metric][2] == 1.0:
                        label = f"{value:.2f}"
                        text_color = "white" if value >= 0.45 else "black"
                    elif value >= 1000:
                        label = f"{value:.3g}"
                        text_color = "white" if value > (vmin + vmax) / 2 else "black"
                    elif abs(value) < 10 and metric not in ("tokens_input_total", "tokens_output_total", "wall_clock_seconds", "llm_calls_total"):
                        label = f"{value:.2f}"
                        text_color = "white" if value >= (vmin + vmax) / 2 else "black"
                    else:
                        label = f"{value:.3g}"
                        text_color = "white" if value > (vmin + vmax) / 2 else "black"
                    ax.text(j, i, label, ha="center", va="center", color=text_color, fontsize=10)

            cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
            cbar.set_label(metric)
            fig.tight_layout()
            path = plot_dir / f"{metric}.svg"
            fig.savefig(path, format="svg", bbox_inches="tight")
            plt.close(fig)
            written.append(path)
    return written

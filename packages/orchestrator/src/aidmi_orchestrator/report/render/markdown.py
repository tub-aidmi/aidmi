"""Markdown summary tables."""
from __future__ import annotations

import statistics

from aidmi_orchestrator.report.aggregate import CellAggregate
from aidmi_orchestrator.report.format import fmt_agg


def render_markdown(
    cells: list[CellAggregate],
    headline_metrics: list[str],
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
            cols = [fmt_agg(c.metrics.get(m)) for m in present]
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

"""Aggregate report CSV tables."""
from __future__ import annotations

import csv
from pathlib import Path

from aidmi_orchestrator.report.aggregate import CellAggregate


def write_tables(cells: list[CellAggregate], out_dir: Path) -> None:
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

"""Shared strategy/fixture label helpers for report plots."""
from __future__ import annotations

from typing import Any

from aidmi_orchestrator.report.layout.benchmark_grid import heatmap_row_key, heatmap_row_label


def strategy_label(row: dict[str, Any]) -> str:
    spec = row["strategy_spec_name"]
    strategy = row.get("strategy_name", spec)
    return heatmap_row_label(heatmap_row_key(spec, strategy), strategy)

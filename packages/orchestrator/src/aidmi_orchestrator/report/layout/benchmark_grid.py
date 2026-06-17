"""Benchmark grid layout: strategy row keys and model columns for heatmaps."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aidmi_orchestrator.report.aggregate import CellAggregate

_ROW_SUFFIXES = ("_writer_model_", "_planner_model_")

CELL_BASE_LABELS: dict[str, str] = {
    "critique": "write_then_critique",
    "freeform": "write_tools_freeform",
    "freeform_sc": "write_tools_freeform",
    "structured": "structured_per_table",
    "structured_sc": "structured_per_table_sc",
    "plan": "plan_then_execute",
    "ensemble": "ensemble_vote",
}

ROW_LABEL_ORDER = [
    "write_then_critique",
    "write_tools_freeform",
    "structured_per_table",
    "structured_per_table_sc",
    "plan_then_execute",
    "ensemble_vote",
]

MODEL_ORDER = [
    "academic/openai-gpt-oss-120b",
    "nvidia/mistral-medium-3.5-128b",
    "academic/qwen3.5-397b-a17b",
    "academic/devstral-2-123b-instruct-2512",
    "ise-ollama/qwen3.6:35b-a3b",
    "ise-openai-nvidia/qwen35-9b",
]

MODEL_LABELS: dict[str, str] = {
    "academic/openai-gpt-oss-120b": "gpt-oss-120b",
    "nvidia/mistral-medium-3.5-128b": "mistral-128b",
    "academic/qwen3.5-397b-a17b": "qwen3.5-397b",
    "academic/devstral-2-123b-instruct-2512": "devstral-123b",
    "ise-ollama/qwen3.6:35b-a3b": "qwen3.6-35b",
    "ise-openai-nvidia/qwen35-9b": "qwen3.5-9b",
}


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
    return CELL_BASE_LABELS.get(row_key, strategy_name)


def ordered_row_labels(keys: set[str], cells_by_key: dict[str, str]) -> list[str]:
    labels = {heatmap_row_label(k, cells_by_key[k]) for k in keys}
    ordered = [label for label in ROW_LABEL_ORDER if label in labels]
    ordered.extend(sorted(labels - set(ordered)))
    return ordered


def ordered_models(models: set[str]) -> list[str]:
    ordered = [m for m in MODEL_ORDER if m in models]
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

    row_labels = ordered_row_labels(row_keys, key_to_strategy)
    col_models = ordered_models(models)
    key_by_label = {heatmap_row_label(k, key_to_strategy[k]): k for k in row_keys}

    matrix = np.full((len(row_labels), len(col_models)), np.nan)
    for i, label in enumerate(row_labels):
        row_key = key_by_label[label]
        for j, model in enumerate(col_models):
            value = by_row_model.get((row_key, model))
            if value is not None:
                matrix[i, j] = value

    col_labels = [MODEL_LABELS.get(m, m) for m in col_models]
    return matrix, row_labels, col_labels

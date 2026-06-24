"""MappingAccuracyEvaluator — recall-oriented scoring of the AI mapping manifest
against a case's `_field_mapping` ground truth (ground_truth.json).

The ground truth is partial for some cases (only non-trivial edges documented), so
the metric is recall-oriented: manifest edges to target columns ABSENT from the
ground truth are neither credited nor penalised. Only an edge that contradicts a
known ground-truth edge (a GT-covered target column mapped to a non-accepted source)
counts as a conflict.
"""
from __future__ import annotations

import json
from typing import Any

from dlt.common.normalizers.naming.snake_case import NamingConvention

from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator

_NAMING = NamingConvention()


def _norm(name: str) -> str:
    """Normalize via dlt's snake_case naming convention — the same transformation dlt
    applies on load, so ground-truth original identifiers reconcile with the
    dlt-normalized names a strategy sees via discover. Idempotent for already-snake names."""
    return _NAMING.normalize_identifier(name) if name else ""


def _split_source(entry: str) -> tuple[str | None, str]:
    """A manifest source ref is 'table.column' or bare 'column'. Returns (table|None, column), normalized."""
    parts = entry.split(".")
    if len(parts) >= 2:
        return _norm(parts[-2]), _norm(parts[-1])
    return None, _norm(parts[-1])


def _source_matches(gt_table: str, gt_col: str, entry: str) -> bool:
    e_table, e_col = _split_source(entry)
    if e_col != _norm(gt_col):
        return False
    if e_table is not None and e_table != _norm(gt_table):
        return False
    return True


class MappingAccuracyEvaluator:
    name = "mapping_accuracy"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.ground_truth_mapping_path is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        gt_path = artifacts.fixture.ground_truth_mapping_path
        gt = json.loads(gt_path.read_text(encoding="utf-8"))
        edges = gt.get("edges", [])

        manifest = artifacts.strategy_result.manifest
        if manifest is None:
            return {
                "manifest_present": False,
                "ground_truth_edges": len(edges),
                "ground_truth_transform_edges": None,
                "edge_recall": None,
                "column_recall": None,
                "conflicting_edges": None,
                "unmapped_ground_truth": None,
            }

        predicted: dict[tuple[str, str], list[str]] = {}
        for note in manifest.tables:
            for cn in note.column_notes:
                key = (_norm(note.target_table), _norm(cn.target_column))
                predicted.setdefault(key, []).extend(cn.source_columns)

        gt_by_col: dict[tuple[str, str], list[dict]] = {}
        for e in edges:
            key = (_norm(e["target_table"]), _norm(e["target_column"]))
            gt_by_col.setdefault(key, []).append(e)

        edge_hits = 0
        transform_edges = 0
        column_hits = 0
        unmapped: list[str] = []

        for key, gt_edges in gt_by_col.items():
            preds = predicted.get(key, [])
            col_has_hit = False
            for e in gt_edges:
                if e.get("transform"):
                    transform_edges += 1
                if any(_source_matches(e["source_table"], e["source_column"], p) for p in preds):
                    edge_hits += 1
                    col_has_hit = True
            if col_has_hit:
                column_hits += 1
            else:
                unmapped.append(f"{gt_edges[0]['target_table']}.{gt_edges[0]['target_column']}")

        conflicting = 0
        for key, preds in predicted.items():
            if key not in gt_by_col:
                continue
            accepted = gt_by_col[key]
            for p in preds:
                if not any(_source_matches(e["source_table"], e["source_column"], p) for e in accepted):
                    conflicting += 1

        total_edges = len(edges)
        total_cols = len(gt_by_col)
        return {
            "manifest_present": True,
            "ground_truth_edges": total_edges,
            "ground_truth_transform_edges": transform_edges,
            "edge_recall": edge_hits / total_edges if total_edges else None,
            "column_recall": column_hits / total_cols if total_cols else None,
            "conflicting_edges": conflicting,
            "unmapped_ground_truth": unmapped,
        }


register_evaluator("mapping_accuracy", MappingAccuracyEvaluator)

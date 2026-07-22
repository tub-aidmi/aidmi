"""GroundTruthFieldAccuracyEvaluator — column match rate on legacy-matched rows."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator._ground_truth_utils import (
    TARGET_TABLES,
    compare_matched_rows,
    fetch_table_rows,
    index_by_column,
    legacy_id_column,
    safe_rate,
    schema_has_table,
)
from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class GroundTruthFieldAccuracyEvaluator:
    name = "ground_truth_field_accuracy"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.golden_schema is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        golden_schema = artifacts.fixture.golden_schema
        assert golden_schema is not None

        per_table: dict[str, dict[str, Any]] = {}
        total_matches = 0
        total_comparisons = 0

        with psycopg2.connect(artifacts.staging_db_url) as conn:
            for table in TARGET_TABLES:
                if table not in artifacts.strategy_result.target_tables_written:
                    continue
                legacy_col = legacy_id_column(table)
                if legacy_col is None:
                    continue
                if not schema_has_table(conn, golden_schema, table):
                    continue
                if not schema_has_table(conn, artifacts.out_schema, table):
                    continue

                golden_rows = fetch_table_rows(conn, golden_schema, table)
                produced_rows = fetch_table_rows(conn, artifacts.out_schema, table)
                produced_by_legacy = index_by_column(produced_rows, legacy_col)

                column_hits: dict[str, int] = defaultdict(int)
                column_totals: dict[str, int] = defaultdict(int)
                table_matches = 0
                table_comparisons = 0

                for golden_row in golden_rows:
                    legacy_id = golden_row.get(legacy_col)
                    if legacy_id is None:
                        continue
                    produced_row = produced_by_legacy.get(str(legacy_id))
                    if produced_row is None:
                        continue

                    col_results = compare_matched_rows(golden_row, produced_row)
                    for col, is_match in col_results.items():
                        column_totals[col] += 1
                        if is_match:
                            column_hits[col] += 1
                        table_comparisons += 1
                        if is_match:
                            table_matches += 1

                per_column = {
                    col: safe_rate(column_hits[col], column_totals[col])
                    for col in sorted(column_totals)
                }
                per_table[table] = {
                    "overall": safe_rate(table_matches, table_comparisons),
                    "per_column": per_column,
                    "compared_cells": table_comparisons,
                    "matched_cells": table_matches,
                }

                total_matches += table_matches
                total_comparisons += table_comparisons

        return {
            "gt_field_accuracy_overall": safe_rate(total_matches, total_comparisons),
            "gt_field_accuracy_per_table": per_table,
        }


register_evaluator("ground_truth_field_accuracy", GroundTruthFieldAccuracyEvaluator)

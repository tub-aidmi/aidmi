"""GroundTruthRecallEvaluator — row-level recall/precision/F1 vs golden schema."""

from __future__ import annotations

from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator._ground_truth_utils import (
    TARGET_TABLES,
    harmonic_mean_f1,
    legacy_id_column,
    match_produced_to_golden,
    safe_rate,
    schema_has_table,
    fetch_table_rows,
)
from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class GroundTruthRecallEvaluator:
    name = "ground_truth_recall"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.golden_schema is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        golden_schema = artifacts.fixture.golden_schema
        assert golden_schema is not None

        per_table: dict[str, dict[str, Any]] = {}
        total_matched = 0
        total_produced = 0
        total_golden = 0
        tables_materialized = 0

        with psycopg2.connect(artifacts.staging_db_url) as conn:
            for table in TARGET_TABLES:
                if schema_has_table(conn, artifacts.out_schema, table):
                    tables_materialized += 1

            for table in TARGET_TABLES:
                if table not in artifacts.strategy_result.target_tables_written:
                    continue
                legacy_col = legacy_id_column(table)
                if legacy_col is None:
                    continue
                if not schema_has_table(conn, golden_schema, table):
                    continue
                if not schema_has_table(conn, artifacts.out_schema, table):
                    golden_rows = fetch_table_rows(conn, golden_schema, table)
                    _, _, golden_n = match_produced_to_golden(
                        golden_rows, [], legacy_col
                    )
                    per_table[table] = {
                        "expected": golden_n,
                        "produced": 0,
                        "matched": 0,
                        "recall": safe_rate(0, golden_n),
                        "precision": safe_rate(0, 0),
                        "f1": 0.0 if golden_n else None,
                        "missing_table": True,
                    }
                    total_golden += golden_n
                    continue

                golden_rows = fetch_table_rows(conn, golden_schema, table)
                produced_rows = fetch_table_rows(conn, artifacts.out_schema, table)
                matched, produced_n, golden_n = match_produced_to_golden(
                    golden_rows, produced_rows, legacy_col
                )

                recall = safe_rate(matched, golden_n)
                precision = safe_rate(matched, produced_n)
                per_table[table] = {
                    "expected": golden_n,
                    "produced": produced_n,
                    "matched": matched,
                    "recall": recall,
                    "precision": precision,
                    "f1": harmonic_mean_f1(recall or 0.0, precision or 0.0)
                    if recall is not None and precision is not None
                    else None,
                }

                total_matched += matched
                total_produced += produced_n
                total_golden += golden_n

        overall_recall = safe_rate(total_matched, total_golden)
        overall_precision = safe_rate(total_matched, total_produced)
        overall_f1 = (
            harmonic_mean_f1(overall_recall, overall_precision)
            if overall_recall is not None and overall_precision is not None
            else None
        )
        tables_materialized_rate = safe_rate(tables_materialized, len(TARGET_TABLES))

        return {
            "gt_recall_overall": overall_recall,
            "gt_precision_overall": overall_precision,
            "gt_f1_overall": overall_f1,
            "gt_tables_materialized": tables_materialized_rate,
            "gt_per_table": per_table,
        }


register_evaluator("ground_truth_recall", GroundTruthRecallEvaluator)

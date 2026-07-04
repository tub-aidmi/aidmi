"""GroundTruthNotesEvaluator — recall breakdown by ground-truth note category."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator._ground_truth_utils import (
    TARGET_TABLES,
    fetch_ground_truth_rows,
    fetch_table_rows,
    ground_truth_row_matched,
    index_by_column,
    legacy_id_column,
    note_categories,
    safe_rate,
    schema_has_table,
)
from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class GroundTruthNotesEvaluator:
    name = "ground_truth_notes"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.golden_schema is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        golden_schema = artifacts.fixture.golden_schema
        assert golden_schema is not None

        produced_indexes: dict[str, tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]] = {}

        with psycopg2.connect(artifacts.staging_db_url) as conn:
            gt_rows = fetch_ground_truth_rows(conn, golden_schema)

            for table in TARGET_TABLES:
                if table not in artifacts.strategy_result.target_tables_written:
                    continue
                legacy_col = legacy_id_column(table)
                if legacy_col is None:
                    continue
                if not schema_has_table(conn, artifacts.out_schema, table):
                    produced_indexes[table] = ({}, {})
                    continue
                produced_rows = fetch_table_rows(conn, artifacts.out_schema, table)
                produced_indexes[table] = (
                    index_by_column(produced_rows, legacy_col),
                    index_by_column(produced_rows, "Id"),
                )

            by_category: dict[str, dict[str, int]] = defaultdict(
                lambda: {"expected": 0, "matched": 0}
            )

            for gt_row in gt_rows:
                table = gt_row["target_table"]
                if table not in produced_indexes:
                    continue
                produced_by_legacy, produced_by_id = produced_indexes[table]
                matched = ground_truth_row_matched(
                    gt_row,
                    produced_by_legacy=produced_by_legacy,
                    produced_by_id=produced_by_id,
                )
                for category in note_categories(gt_row.get("notes")):
                    by_category[category]["expected"] += 1
                    if matched:
                        by_category[category]["matched"] += 1

        gt_notes_by_category = {
            category: {
                "expected": stats["expected"],
                "matched": stats["matched"],
                "recall": safe_rate(stats["matched"], stats["expected"]),
            }
            for category, stats in sorted(by_category.items())
        }

        return {"gt_notes_by_category": gt_notes_by_category}


register_evaluator("ground_truth_notes", GroundTruthNotesEvaluator)

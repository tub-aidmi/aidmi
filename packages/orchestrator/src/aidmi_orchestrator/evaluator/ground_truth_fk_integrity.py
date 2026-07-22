"""GroundTruthFkIntegrityEvaluator — foreign-key correctness on matched rows.

FK columns hold surrogate parent Ids that differ between the golden and produced
id spaces, so raw comparison is meaningless. Each FK is instead resolved through
its parent's legacy id and compared on that stable identity: does the child point
at the correct parent? A produced FK pointing at a nonexistent produced parent row
is a dangling reference and counts as a miss.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator._ground_truth_utils import (
    DANGLING,
    TARGET_TABLES,
    fetch_table_rows,
    fk_columns,
    index_by_column,
    index_by_id,
    legacy_id_column,
    resolve_fk,
    safe_rate,
    schema_has_table,
)
from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class GroundTruthFkIntegrityEvaluator:
    name = "ground_truth_fk_integrity"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.golden_schema is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        golden_schema = artifacts.fixture.golden_schema
        assert golden_schema is not None

        per_table: dict[str, dict[str, Any]] = {}
        total_matches = 0
        total_comparisons = 0
        total_dangling = 0

        with psycopg2.connect(artifacts.staging_db_url) as conn:
            parent_index_cache: dict[tuple[str, str], dict[str, str]] = {}

            def parent_index(schema: str, parent: str) -> dict[str, str]:
                key = (schema, parent)
                if key not in parent_index_cache:
                    legacy_col = legacy_id_column(parent)
                    if legacy_col is None or not schema_has_table(conn, schema, parent):
                        parent_index_cache[key] = {}
                    else:
                        rows = fetch_table_rows(conn, schema, parent)
                        parent_index_cache[key] = index_by_id(rows, legacy_col)
                return parent_index_cache[key]

            for table in TARGET_TABLES:
                fks = fk_columns(table)
                if not fks:
                    continue
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
                column_dangling: dict[str, int] = defaultdict(int)
                table_matches = 0
                table_comparisons = 0

                for golden_row in golden_rows:
                    legacy_id = golden_row.get(legacy_col)
                    if legacy_id is None:
                        continue
                    produced_row = produced_by_legacy.get(str(legacy_id))
                    if produced_row is None:
                        continue

                    for fk_col, parent in fks.items():
                        if fk_col not in produced_row:
                            continue
                        golden_res = resolve_fk(
                            golden_row.get(fk_col), parent_index(golden_schema, parent)
                        )
                        produced_res = resolve_fk(
                            produced_row.get(fk_col),
                            parent_index(artifacts.out_schema, parent),
                        )
                        is_dangling = produced_res is DANGLING
                        is_match = (
                            not is_dangling
                            and golden_res is not DANGLING
                            and produced_res == golden_res
                        )
                        column_totals[fk_col] += 1
                        table_comparisons += 1
                        if is_match:
                            column_hits[fk_col] += 1
                            table_matches += 1
                        if is_dangling:
                            column_dangling[fk_col] += 1
                            total_dangling += 1

                if not column_totals:
                    continue

                per_column = {
                    col: safe_rate(column_hits[col], column_totals[col])
                    for col in sorted(column_totals)
                }
                per_table[table] = {
                    "overall": safe_rate(table_matches, table_comparisons),
                    "per_column": per_column,
                    "dangling_per_column": {
                        col: column_dangling[col] for col in sorted(column_dangling)
                    },
                    "compared_cells": table_comparisons,
                    "matched_cells": table_matches,
                }
                total_matches += table_matches
                total_comparisons += table_comparisons

        return {
            "gt_fk_integrity_overall": safe_rate(total_matches, total_comparisons),
            "gt_fk_integrity_per_table": per_table,
            "gt_fk_dangling_total": total_dangling,
        }


register_evaluator("ground_truth_fk_integrity", GroundTruthFkIntegrityEvaluator)

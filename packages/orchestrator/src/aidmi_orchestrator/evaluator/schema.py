"""SchemaEvaluator — three modes per spec Section 8.3."""

from __future__ import annotations

from collections import Counter
from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


def _introspect(db_url: str, schema: str, table: str) -> list[tuple[str, str]]:
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            return [(c, t) for c, t in cur.fetchall()]


def _types_compatible(a: str, b: str) -> bool:
    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return True
    text_family = {"text", "character varying", "varchar", "character", "char"}
    int_family = {"integer", "int", "int4", "bigint", "int8", "smallint", "int2"}
    if a_l in text_family and b_l in text_family:
        return True
    if a_l in int_family and b_l in int_family:
        return True
    return False


class SchemaEvaluator:
    name = "schema"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return True

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        produced_total = 0
        type_histogram: Counter[str] = Counter()
        target = (
            artifacts.target_schema_input or artifacts.strategy_result.target_schema
        )
        covered = 0
        target_total = 0
        extraneous = 0
        type_mismatches = 0

        target_by_table = (
            {t.name: t for t in target.tables} if target is not None else {}
        )

        for tname in artifacts.strategy_result.target_tables_written:
            cols = _introspect(artifacts.staging_db_url, artifacts.out_schema, tname)
            produced_total += len(cols)
            for _, dtype in cols:
                type_histogram[dtype] += 1

            if tname in target_by_table:
                target_cols = {
                    c.name: c.sql_type for c in target_by_table[tname].columns
                }
                produced_cols = {c: t for c, t in cols}
                target_total += len(target_cols)
                for tc, tt in target_cols.items():
                    if tc in produced_cols:
                        covered += 1
                        if not _types_compatible(tt, produced_cols[tc]):
                            type_mismatches += 1
                for pc in produced_cols:
                    if pc not in target_cols:
                        extraneous += 1

        out: dict[str, Any] = {
            "produced_column_count": produced_total,
            "produced_type_histogram": dict(type_histogram),
        }
        if target is not None and target_total > 0:
            out["target_columns_covered"] = covered / target_total
            out["extraneous_columns"] = extraneous
            out["type_mismatches"] = type_mismatches
        else:
            out["target_columns_covered"] = None
            out["extraneous_columns"] = None
            out["type_mismatches"] = None
        return out


register_evaluator("schema", SchemaEvaluator)

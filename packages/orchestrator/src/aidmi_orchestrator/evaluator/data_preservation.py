"""DataPreservationEvaluator — ground-truth-free lossiness signals via SQL aggregates."""
from __future__ import annotations
from typing import Any

import psycopg2

from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator

HIGH_NULL_INFLATION_THRESHOLD = 0.05


def _schema_columns(cur, schema: str) -> dict[str, list[str]]:
    cur.execute(
        """
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = %s ORDER BY table_name, ordinal_position
        """,
        (schema,),
    )
    out: dict[str, list[str]] = {}
    for table, column in cur.fetchall():
        out.setdefault(table, []).append(column)
    return {t: cols for t, cols in out.items() if not t.startswith("_dlt")}


def _row_count(cur, schema: str, table: str) -> int:
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    return cur.fetchone()[0]


def _col_stats(cur, schema: str, table: str, column: str) -> tuple[int, int, int]:
    cur.execute(
        f'SELECT COUNT(*), COUNT("{column}"), COUNT(DISTINCT "{column}") '
        f'FROM "{schema}"."{table}"'
    )
    n, non_null, distinct = cur.fetchone()
    return n, non_null, distinct


def resolve_source_column(entry: str, raw_tables: dict[str, list[str]]) -> tuple[str, str] | None:
    if "." in entry:
        table_part, column = entry.rsplit(".", 1)
        table = table_part.split(".")[-1]
        if table in raw_tables and column in raw_tables[table]:
            return table, column
        return None
    matches = [(t, entry) for t, cols in raw_tables.items() if entry in cols]
    return matches[0] if len(matches) == 1 else None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


class DataPreservationEvaluator:
    name = "data_preservation"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return bool(artifacts.strategy_result.target_tables_written)

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        manifest = artifacts.strategy_result.manifest
        mode = "manifest_mapped" if manifest is not None else "aggregate_only"

        with psycopg2.connect(artifacts.staging_db_url) as conn:
            with conn.cursor() as cur:
                raw_tables = _schema_columns(cur, artifacts.staging_raw_dataset)
                out_tables = _schema_columns(cur, artifacts.staging_out_dataset)
                raw_counts = {t: _row_count(cur, artifacts.staging_raw_dataset, t) for t in raw_tables}
                total_raw = sum(raw_counts.values())

                notes_by_table = (
                    {n.target_table: n for n in manifest.tables} if manifest is not None else {}
                )

                per_table: dict[str, dict[str, Any]] = {}
                empty_tables = 0
                row_ratios: list[float] = []
                for t in artifacts.strategy_result.target_tables_written:
                    if t not in out_tables:
                        continue
                    out_n = _row_count(cur, artifacts.staging_out_dataset, t)
                    if out_n == 0:
                        empty_tables += 1
                    note = notes_by_table.get(t)
                    if note is not None:
                        src_names = [s for s in note.source_tables if s in raw_tables]
                        src_n = sum(raw_counts[s] for s in src_names) if src_names else None
                    else:
                        src_n = total_raw or None
                    ratio = out_n / src_n if src_n else None
                    if ratio is not None:
                        row_ratios.append(ratio)
                    per_table[t] = {"rows": out_n, "source_rows": src_n, "row_ratio": ratio}

                null_inflations: list[float] = []
                distinct_ratios: list[float] = []
                unresolved = 0
                if manifest is not None:
                    for note in manifest.tables:
                        if note.target_table not in out_tables:
                            continue
                        for cn in note.column_notes:
                            if len(cn.source_columns) != 1:
                                continue
                            resolved = resolve_source_column(cn.source_columns[0], raw_tables)
                            if resolved is None:
                                unresolved += 1
                                continue
                            if cn.target_column not in out_tables[note.target_table]:
                                continue
                            s_table, s_col = resolved
                            sn, s_non_null, s_distinct = _col_stats(
                                cur, artifacts.staging_raw_dataset, s_table, s_col)
                            tn, t_non_null, t_distinct = _col_stats(
                                cur, artifacts.staging_out_dataset, note.target_table, cn.target_column)
                            if sn and tn:
                                null_inflations.append((1 - t_non_null / tn) - (1 - s_non_null / sn))
                                if s_distinct:
                                    distinct_ratios.append(t_distinct / s_distinct)

        return {
            "preservation_mode": mode,
            "preservation_row_ratio_mean": _mean(row_ratios),
            "preservation_empty_tables": empty_tables,
            "preservation_null_inflation_mean": _mean(null_inflations),
            "preservation_null_inflation_max": max(null_inflations) if null_inflations else None,
            "preservation_high_null_inflation_columns": (
                sum(1 for v in null_inflations if v > HIGH_NULL_INFLATION_THRESHOLD)
                if null_inflations else None
            ),
            "preservation_distinct_ratio_mean": _mean(distinct_ratios),
            "preservation_distinct_ratio_min": min(distinct_ratios) if distinct_ratios else None,
            "preservation_unresolved_mappings": unresolved,
            "preservation_per_table": per_table,
        }


register_evaluator("data_preservation", DataPreservationEvaluator)

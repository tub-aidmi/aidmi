"""Shared helpers for ground-truth evaluators."""
from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

LEGACY_ID_COLUMNS: dict[str, str] = {
    "Account": "Legacy_Customer_ID__c",
    "Contact": "Legacy_Contact_ID__c",
    "Opportunity": "Legacy_Opportunity_ID__c",
    "Project__c": "Legacy_Project_ID__c",
    "Installed_Asset__c": "Legacy_Asset_ID__c",
}

TARGET_TABLES: tuple[str, ...] = tuple(LEGACY_ID_COLUMNS.keys())


def legacy_id_column(table_name: str) -> str | None:
    return LEGACY_ID_COLUMNS.get(table_name)


def harmonic_mean_f1(recall: float, precision: float) -> float:
    if recall + precision == 0:
        return 0.0
    return 2 * recall * precision / (recall + precision)


def safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def fetch_table_rows(conn, schema: str, table: str) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f'SELECT * FROM "{schema}"."{table}"')
        return [dict(row) for row in cur.fetchall()]


def fetch_ground_truth_rows(conn, golden_schema: str) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f'SELECT target_table, target_id, source_table, source_id, notes '
            f'FROM "{golden_schema}"._ground_truth'
        )
        return [dict(row) for row in cur.fetchall()]


def schema_has_table(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def index_by_column(rows: list[dict[str, Any]], column: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = row.get(column)
        if value is not None:
            out[str(value)] = row
    return out


def legacy_id_set(rows: list[dict[str, Any]], legacy_col: str) -> set[str]:
    return {str(row[legacy_col]) for row in rows if row.get(legacy_col) is not None}


def match_produced_to_golden(
    golden_rows: list[dict[str, Any]],
    produced_rows: list[dict[str, Any]],
    legacy_col: str,
) -> tuple[int, int, int]:
    """Return (matched, produced_count, golden_count) keyed by legacy ID."""
    golden_ids = legacy_id_set(golden_rows, legacy_col)
    produced_ids = legacy_id_set(produced_rows, legacy_col)
    matched = len(golden_ids & produced_ids)
    return matched, len(produced_ids), len(golden_ids)


def parse_note_tags(notes: str | None) -> list[str]:
    if not notes:
        return []
    return [tag for tag in notes.split(";") if tag]


def note_categories(notes: str | None) -> list[str]:
    tags = parse_note_tags(notes)
    return tags if tags else ["clean"]


def ground_truth_row_matched(
    gt_row: dict[str, Any],
    *,
    produced_by_legacy: dict[str, dict[str, Any]],
    produced_by_id: dict[str, dict[str, Any]],
) -> bool:
    """Return whether the strategy output satisfies this ground-truth row."""
    notes = gt_row.get("notes")
    source_id = gt_row.get("source_id")
    target_id = gt_row.get("target_id")

    if source_id is None:
        return False

    tags = parse_note_tags(notes)
    if "duplicate_account" in tags:
        return str(target_id) in produced_by_id

    return str(source_id) in produced_by_legacy


def values_equal(produced: Any, golden: Any) -> bool:
    if produced is None and golden is None:
        return True
    if produced is None or golden is None:
        return False
    if isinstance(produced, float) and isinstance(golden, float):
        return abs(produced - golden) < 1e-9
    return str(produced) == str(golden)


def compare_matched_rows(
    golden_row: dict[str, Any],
    produced_row: dict[str, Any],
    *,
    skip_columns: frozenset[str] = frozenset({"Id", "CreatedDate", "LastModifiedDate", "IsDeleted"}),
) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for col, golden_val in golden_row.items():
        if col in skip_columns:
            continue
        if col not in produced_row:
            continue
        results[col] = values_equal(produced_row[col], golden_val)
    return results

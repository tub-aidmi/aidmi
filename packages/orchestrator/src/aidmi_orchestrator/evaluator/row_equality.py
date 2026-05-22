"""RowEqualityEvaluator — runs reference dbt in a sibling schema, compares row sets."""
from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import psycopg2
from psycopg2.extras import RealDictCursor

from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class Comparator(Protocol):
    def compare_row(self, produced: dict, reference: dict) -> bool: ...


@dataclass
class ExactComparator:
    def compare_row(self, produced: dict, reference: dict) -> bool:
        return produced == reference


@dataclass
class FuzzyComparator:
    normalize_whitespace: bool = True
    case_insensitive: bool = False
    ignore_trailing_nulls: bool = True

    def _normalize(self, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            if self.normalize_whitespace:
                v = re.sub(r"\s+", " ", v).strip()
            if self.case_insensitive:
                v = v.lower()
        return v

    def compare_row(self, produced: dict, reference: dict) -> bool:
        keys = set(produced) | set(reference)
        for k in keys:
            p = self._normalize(produced.get(k))
            r = self._normalize(reference.get(k))
            if self.ignore_trailing_nulls and p is None and r is None:
                continue
            if p != r:
                return False
        return True


def _run_reference_dbt(
    db_url: str, dataset: str, reference_path: Path, pipelines_dir: Path
) -> str:
    """Run the reference dbt project, materializing into <dataset>_reference."""
    import dlt
    ref_dataset = f"{dataset}_reference"
    pipeline = dlt.pipeline(
        pipeline_name=f"ref_{ref_dataset}",
        pipelines_dir=str(pipelines_dir),
        destination=dlt.destinations.postgres(db_url),
        dataset_name=ref_dataset,
    )
    # The reference dbt project reads from the original `dataset` (where raw
    # source data lives); we point dbt's `target.schema` at `ref_dataset` so
    # outputs materialize there, but the source('source_crm', 'contacts')
    # reference resolves to `ref_dataset.contacts`. To keep the reference
    # tables visible to dbt, we COPY the raw tables into ref_dataset first.
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{ref_dataset}"')
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name NOT LIKE %s ESCAPE %s
                """,
                (dataset, r"\_dlt%", "\\"),
            )
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(
                    f'CREATE TABLE IF NOT EXISTS "{ref_dataset}"."{t}" AS '
                    f'SELECT * FROM "{dataset}"."{t}"'
                )

    venv = dlt.dbt.get_venv(pipeline, venv_path="")
    runner = dlt.dbt.package(pipeline, str(reference_path), venv=venv)
    runner.run_all()
    return ref_dataset


class RowEqualityEvaluator:
    name = "row_equality"

    def __init__(self, comparator: Comparator | None = None):
        self.comparator = comparator or ExactComparator()

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return artifacts.fixture.reference_dbt_path is not None

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        ref_dataset = _run_reference_dbt(
            artifacts.staging_db_url,
            artifacts.staging_dataset,
            artifacts.fixture.reference_dbt_path,
            artifacts.dlt_pipelines_dir,
        )

        per_table: dict[str, dict[str, Any]] = {}
        any_mismatch = False
        for tname in artifacts.strategy_result.target_tables_written:
            with psycopg2.connect(artifacts.staging_db_url) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(f'SELECT * FROM "{artifacts.staging_dataset}"."{tname}"')
                    produced = [dict(r) for r in cur.fetchall()]
                    cur.execute(f'SELECT * FROM "{ref_dataset}"."{tname}"')
                    reference = [dict(r) for r in cur.fetchall()]

            # Compare unordered by key (use target's primary key heuristically:
            # first column). For v1 simplicity, sort by all keys.
            def _key(r: dict):
                return tuple(sorted(r.items(), key=lambda kv: kv[0]))
            produced_sorted = sorted(produced, key=_key)
            reference_sorted = sorted(reference, key=_key)

            row_count_match = len(produced_sorted) == len(reference_sorted)
            diff_count = abs(len(produced_sorted) - len(reference_sorted))
            for p, r in zip(produced_sorted, reference_sorted):
                if not self.comparator.compare_row(p, r):
                    diff_count += 1

            if not row_count_match or diff_count > 0:
                any_mismatch = True

            n_comparable = min(len(produced_sorted), len(reference_sorted))
            if n_comparable == 0:
                column_value_match_rate = None
            else:
                all_cols: set[str] = set()
                for row in produced_sorted[:n_comparable]:
                    all_cols.update(row.keys())
                for row in reference_sorted[:n_comparable]:
                    all_cols.update(row.keys())
                column_value_match_rate = {}
                for col in all_cols:
                    matches = sum(
                        1 for p, r in zip(produced_sorted[:n_comparable], reference_sorted[:n_comparable])
                        if p.get(col) == r.get(col)
                    )
                    column_value_match_rate[col] = matches / n_comparable

            per_table[tname] = {
                "row_count_match": row_count_match,
                "row_set_diff_count": diff_count,
                "produced_rows": len(produced_sorted),
                "reference_rows": len(reference_sorted),
                "column_value_match_rate": column_value_match_rate,
            }

        return {
            "row_count_match": all(t["row_count_match"] for t in per_table.values()),
            "row_set_diff_count": sum(t["row_set_diff_count"] for t in per_table.values()),
            "per_table_equality": per_table,
            "any_table_mismatch": any_mismatch,
        }


register_evaluator("row_equality", RowEqualityEvaluator)

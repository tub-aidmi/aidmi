"""Postgres introspection → SourceSummary."""
from __future__ import annotations
import psycopg2
from psycopg2.extras import RealDictCursor

from aidmi_orchestrator.domain import ColumnInfo, TableInfo, SourceSummary


def discover(db_url: str, dataset_name: str, samples_per_table: int = 100) -> SourceSummary:
    tables: list[TableInfo] = []
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name NOT LIKE %s ESCAPE %s
                ORDER BY table_name
                """,
                (dataset_name, r"\_dlt%", "\\"),
            )
            table_names = [r[0] for r in cur.fetchall()]

        for table_name in table_names:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (dataset_name, table_name),
                )
                columns = [
                    ColumnInfo(name=c, sql_type=t, nullable=(n == "YES"))
                    for c, t, n in cur.fetchall()
                ]

            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{dataset_name}"."{table_name}"')
                row_count = cur.fetchone()[0]

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f'SELECT * FROM "{dataset_name}"."{table_name}" LIMIT %s',
                    (samples_per_table,),
                )
                sample_rows = [dict(r) for r in cur.fetchall()]

            tables.append(TableInfo(
                db_schema=dataset_name,
                name=table_name,
                columns=columns,
                row_count=row_count,
                sample_rows=sample_rows,
            ))

    return SourceSummary(tables=tables)

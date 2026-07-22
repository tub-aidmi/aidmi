"""Load fixture source.sql files into Postgres staging schemas."""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import aidmi_orchestrator.fixtures  # noqa: F401 — register fixtures
from aidmi_orchestrator.fixtures.base import get_fixture, list_fixtures


def _db_url_from_env() -> str:
    url = os.environ.get("AIDMI_STAGING_DB_URL")
    if url:
        return url
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "test")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def init_fixture(name: str, db_url: str) -> None:
    fixture = get_fixture(name)

    sql = fixture.source_sql_path.read_text(encoding="utf-8")
    with psycopg2.connect(db_url) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {fixture.source_schema} CASCADE")
            cur.execute(sql)

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """,
                (fixture.source_schema,),
            )
            table_count = cur.fetchone()[0]

    print(f"initialized {name} → schema {fixture.source_schema} ({table_count} tables)")

    if fixture.golden_schema and fixture.destination_sql_path.exists():
        golden_sql = fixture.destination_sql_path.read_text(encoding="utf-8")
        with psycopg2.connect(db_url) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {fixture.golden_schema} CASCADE")
                cur.execute(golden_sql)
        print(f"  loaded golden → schema {fixture.golden_schema}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "fixtures",
        nargs="*",
        help="fixture names (default: all SQL fixtures)",
    )
    parser.add_argument(
        "--db-url", default=None, help="Postgres URL (default: from env)"
    )
    args = parser.parse_args(argv)

    db_url = args.db_url or _db_url_from_env()
    names = args.fixtures or list_fixtures()
    if not names:
        raise SystemExit("no SQL fixtures to initialize")

    for name in names:
        init_fixture(name, db_url)


if __name__ == "__main__":
    main(sys.argv[1:])

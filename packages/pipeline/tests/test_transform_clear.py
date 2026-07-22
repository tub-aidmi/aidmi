"""Tests for transform schema clearing."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from aidmi_pipeline.config import MigrationRun, StagingConfig
from aidmi_pipeline.migration import clear_out_schema, transform


@pytest.fixture
def staging_db_url():
    import os

    url = os.environ.get("AIDMI_STAGING_DB_URL")
    if url:
        return url
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "test")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def test_clear_out_schema_drops_existing_tables(staging_db_url):
    schema = "test_clear_out_schema_tmp"
    clear_out_schema(staging_db_url, schema)
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{schema}"."t" (id int)')
            conn.commit()
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = 't'",
                (schema,),
            )
            assert cur.fetchone() is not None

    clear_out_schema(staging_db_url, schema)
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = 't'",
                (schema,),
            )
            assert cur.fetchone() is None

    with psycopg2.connect(staging_db_url) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_transform_clears_schema_before_dbt(tmp_path, staging_db_url):
    schema = "test_transform_clear_tmp"
    dbt_path = tmp_path / "dbt"
    models = dbt_path / "models"
    models.mkdir(parents=True)
    (models / "sources.yml").write_text(
        "version: 2\nsources:\n  - name: src\n    schema: raw\n    tables:\n      - name: t\n",
        encoding="utf-8",
    )
    (models / "m.sql").write_text(
        "{{ config(materialized='table') }}\nSELECT 1 AS id\n",
        encoding="utf-8",
    )

    run = MigrationRun(
        source=None,
        staging=StagingConfig.for_run(staging_db_url, "raw_unused", schema),
        target=None,
        target_dataset="",
        target_tables=[],
        dbt_project_path=dbt_path,
    )

    clear_out_schema(staging_db_url, schema)
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{schema}"."stale" (id int)')
            conn.commit()

    mock_outcome = SimpleNamespace(
        model_name="m",
        status="success",
        message=None,
        time=0.0,
    )
    mock_runner = MagicMock()
    mock_runner.run_all.return_value = [mock_outcome]

    with patch("aidmi_pipeline.migration.dlt") as mock_dlt:
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.dbt.get_venv.return_value = MagicMock()
        mock_dlt.dbt.package.return_value = mock_runner
        transform(run)

    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s",
                (schema,),
            )
            tables = {r[0] for r in cur.fetchall()}
    assert "stale" not in tables

    with psycopg2.connect(staging_db_url) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

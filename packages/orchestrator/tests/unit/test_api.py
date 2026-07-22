import psycopg2
import pytest

from aidmi_orchestrator.api import OrchestratorAPI
from aidmi_orchestrator.domain import SourceSummary
from aidmi_orchestrator.trace import TraceSink


def _seed(db_url: str, schema: str):
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}".t (a INTEGER, b TEXT)')
            cur.executemany(
                f'INSERT INTO "{schema}".t (a, b) VALUES (%s, %s)',
                [(1, "x"), (2, "y"), (3, "z")],
            )


def test_read_table_sample(staging_db_url, tmp_path):
    _seed(staging_db_url, "src_api1")
    trace = TraceSink(tmp_path / "trace.jsonl")
    api = OrchestratorAPI(
        source_summary=SourceSummary(tables=[]),
        target_schema=None,
        dbt_project_path=tmp_path / "dbt",
        staging_db_url=staging_db_url,
        source_schema="",
        out_schema="",
        trace=trace,
        _pipeline_run=None,
    )
    rows = api.read_table_sample("src_api1", "t", n=2)
    assert len(rows) == 2
    assert {r["a"] for r in rows} <= {1, 2, 3}


def test_query_postgres_rejects_non_select(staging_db_url, tmp_path):
    _seed(staging_db_url, "src_api2")
    trace = TraceSink(tmp_path / "trace.jsonl")
    api = OrchestratorAPI(
        source_summary=SourceSummary(tables=[]),
        target_schema=None,
        dbt_project_path=tmp_path / "dbt",
        staging_db_url=staging_db_url,
        source_schema="",
        out_schema="",
        trace=trace,
        _pipeline_run=None,
    )
    with pytest.raises(ValueError, match="only SELECT"):
        api.query_postgres('INSERT INTO "src_api2".t (a, b) VALUES (99, "no")')


def test_query_postgres_row_cap(staging_db_url, tmp_path):
    _seed(staging_db_url, "src_api3")
    trace = TraceSink(tmp_path / "trace.jsonl")
    api = OrchestratorAPI(
        source_summary=SourceSummary(tables=[]),
        target_schema=None,
        dbt_project_path=tmp_path / "dbt",
        staging_db_url=staging_db_url,
        source_schema="",
        out_schema="",
        trace=trace,
        _pipeline_run=None,
    )
    rows = api.query_postgres('SELECT * FROM "src_api3".t', row_cap=1)
    assert len(rows) == 1

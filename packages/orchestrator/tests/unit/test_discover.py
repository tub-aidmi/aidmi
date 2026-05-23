import psycopg2
from aidmi_orchestrator.discover import discover


def _seed(db_url: str, schema: str):
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f'CREATE TABLE "{schema}".contacts (id INTEGER, first_name TEXT, email TEXT NOT NULL)'
            )
            cur.executemany(
                f'INSERT INTO "{schema}".contacts (id, first_name, email) VALUES (%s, %s, %s)',
                [(1, "alice", "a@x.com"), (2, "bob", "b@x.com")],
            )


def test_discover_summarizes_seeded_schema(staging_db_url):
    _seed(staging_db_url, "src_test_discover")

    summary = discover(staging_db_url, "src_test_discover", samples_per_table=2)
    assert len(summary.tables) == 1
    t = summary.tables[0]
    assert t.name == "contacts"
    assert t.db_schema == "src_test_discover"
    assert t.row_count == 2
    cols = {c.name: c for c in t.columns}
    assert cols["id"].sql_type.startswith("integer")
    assert cols["email"].nullable is False
    assert len(t.sample_rows) == 2
    assert t.sample_rows[0]["first_name"] in ("alice", "bob")


def test_discover_skips_dlt_internal_tables(staging_db_url):
    _seed(staging_db_url, "src_test_discover2")
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE "src_test_discover2"._dlt_loads (load_id TEXT)')
            cur.execute('CREATE TABLE "src_test_discover2"._dlt_pipeline_state (state TEXT)')
    summary = discover(staging_db_url, "src_test_discover2", samples_per_table=2)
    table_names = {t.name for t in summary.tables}
    assert "_dlt_loads" not in table_names
    assert "_dlt_pipeline_state" not in table_names
    assert "contacts" in table_names

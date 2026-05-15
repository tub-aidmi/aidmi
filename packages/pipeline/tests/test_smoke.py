import psycopg2


def test_postgres_fixture_starts(staging_db_url):
    assert staging_db_url.startswith("postgresql://")
    with psycopg2.connect(staging_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

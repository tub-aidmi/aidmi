import pytest
from aidmi_orchestrator.cli import staging_db_url_from_env


@pytest.fixture(autouse=True)
def clear_staging_env(monkeypatch):
    for key in (
        "AIDMI_STAGING_DB_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    ):
        monkeypatch.delenv(key, raising=False)


def test_direct_aidmi_url_wins(monkeypatch):
    monkeypatch.setenv("AIDMI_STAGING_DB_URL", "postgresql://a:b@h:1/d")
    monkeypatch.setenv("POSTGRES_USER", "x")
    monkeypatch.setenv("POSTGRES_PASSWORD", "y")
    monkeypatch.setenv("POSTGRES_DB", "z")
    assert staging_db_url_from_env() == "postgresql://a:b@h:1/d"


def test_composed_from_postgres_vars(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "postgres")
    assert (
        staging_db_url_from_env()
        == "postgresql://postgres:test@localhost:5432/postgres"
    )


def test_composed_respects_host_and_port(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "db")
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    assert staging_db_url_from_env() == "postgresql://u:p@db.internal:5433/db"


def test_password_special_chars_quoted(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss:word")
    monkeypatch.setenv("POSTGRES_DB", "db")
    assert staging_db_url_from_env() == "postgresql://u:p%40ss%3Aword@localhost:5432/db"


def test_returns_none_when_incomplete(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "db")
    assert staging_db_url_from_env() is None

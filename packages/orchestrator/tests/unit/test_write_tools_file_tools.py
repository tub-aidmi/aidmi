"""Robustness tests for write_tools_freeform file tool factories."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from aidmi_orchestrator.strategy.write_tools_freeform.tools import (
    make_read_file,
    make_write_file,
)


def test_write_file_to_directory_returns_error_not_raises(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    write_file = make_write_file(api)

    result = asyncio.run(write_file("models", "SELECT 1"))

    assert "ERROR" in result
    assert "models" in result
    api.trace.record.assert_called_once()

    ok = asyncio.run(
        write_file(
            "models/users.sql",
            "{{ config(materialized='table') }}\nSELECT 1 AS \"Id\"\n",
        )
    )
    assert "wrote" in ok
    assert (
        "{{ config(materialized='table') }}"
        in (tmp_path / "models" / "users.sql").read_text()
    )


def test_write_file_path_escape_returns_error_not_raises(tmp_path: Path) -> None:
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    write_file = make_write_file(api)

    result = asyncio.run(write_file("../escape.sql", "x"))

    assert "ERROR" in result
    api.trace.record.assert_called_once()


def test_read_file_on_directory_returns_error_not_raises(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    read_file = make_read_file(api)

    result = asyncio.run(read_file("models"))

    assert "ERROR" in result
    assert "models" in result
    api.trace.record.assert_called_once()


def test_read_file_missing_returns_empty_string(tmp_path: Path) -> None:
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    read_file = make_read_file(api)

    result = asyncio.run(read_file("models/nonexistent.sql"))

    assert result == ""
    api.trace.record.assert_called_once()


def test_read_file_path_escape_returns_error_not_raises(tmp_path: Path) -> None:
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    read_file = make_read_file(api)

    result = asyncio.run(read_file("../escape.sql"))

    assert "ERROR" in result
    api.trace.record.assert_called_once()


def test_write_file_rejects_nested_dbt_project_path(tmp_path: Path) -> None:
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    write_file = make_write_file(api)

    result = asyncio.run(
        write_file(
            "dbt_project/models/Account.sql",
            "{{ config(materialized='table') }}\nSELECT 1 AS \"Id\"\n",
        )
    )

    assert "ERROR" in result
    assert "dbt_project" in result
    assert not (tmp_path / "dbt_project" / "models" / "Account.sql").exists()


def test_discover_model_sql_files_prefers_top_level_models(tmp_path: Path) -> None:
    from aidmi_orchestrator.strategy.base import discover_model_sql_files

    top = tmp_path / "models"
    top.mkdir()
    nested = tmp_path / "dbt_project" / "models"
    nested.mkdir(parents=True)
    (top / "Account.sql").write_text("top", encoding="utf-8")
    (nested / "Contact.sql").write_text("nested", encoding="utf-8")

    files = discover_model_sql_files(tmp_path)
    assert [p.stem for p in files] == ["Account"]


def test_discover_model_sql_files_falls_back_to_nested(tmp_path: Path) -> None:
    from aidmi_orchestrator.strategy.base import discover_model_sql_files

    nested = tmp_path / "dbt_project" / "models"
    nested.mkdir(parents=True)
    (nested / "Account.sql").write_text("nested", encoding="utf-8")

    files = discover_model_sql_files(tmp_path)
    assert [p.stem for p in files] == ["Account"]

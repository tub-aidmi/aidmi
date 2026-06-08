"""Robustness tests for write_tools_freeform file tool factories."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from aidmi_orchestrator.strategy.write_tools_freeform.tools import make_read_file, make_write_file


def test_write_file_to_directory_returns_error_not_raises(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    api = SimpleNamespace(dbt_project_path=tmp_path, trace=MagicMock())
    write_file = make_write_file(api)

    result = asyncio.run(write_file("models", "SELECT 1"))

    assert "ERROR" in result
    assert "models" in result
    api.trace.record.assert_called_once()

    ok = asyncio.run(write_file("models/users.sql", "SELECT 1"))
    assert "wrote" in ok
    assert (tmp_path / "models" / "users.sql").read_text() == "SELECT 1"


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

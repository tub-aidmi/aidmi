"""Fixture dataclass + registry."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass
class Fixture:
    name: str
    description: str
    source_factory: Callable[[], Any]
    target_schema_path: Path | None
    reference_dbt_path: Path | None
    applicable_evaluators: list[str]


_FIXTURES: dict[str, Fixture] = {}


def register_fixture(fixture: Fixture) -> None:
    if fixture.name in _FIXTURES:
        raise ValueError(f"fixture {fixture.name!r} already registered")
    _FIXTURES[fixture.name] = fixture


def list_fixtures() -> list[str]:
    return sorted(_FIXTURES)


def get_fixture(name: str) -> Fixture:
    if name not in _FIXTURES:
        raise ValueError(f"unknown fixture {name!r}. Registered: {list_fixtures()}")
    return _FIXTURES[name]

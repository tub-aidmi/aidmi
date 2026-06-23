import asyncio

import pytest

from aidmi_orchestrator.strategy.base import run_named_coroutines


async def _ok(name: str) -> str:
    return name


async def _boom() -> str:
    raise RuntimeError("fail")


def test_run_named_coroutines_collects_all_failures() -> None:
    with pytest.raises(RuntimeError, match="parallel task\\(s\\) failed"):
        asyncio.run(run_named_coroutines(
            [("a", _ok("a")), ("b", _boom())],
            serial=False,
        ))


def test_run_named_coroutines_returns_mapping() -> None:
    out = asyncio.run(run_named_coroutines(
        [("a", _ok("a")), ("b", _ok("b"))],
        serial=True,
    ))
    assert out == {"a": "a", "b": "b"}

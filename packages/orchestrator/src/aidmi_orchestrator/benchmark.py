"""Benchmark harness: run() with grid expansion."""

from __future__ import annotations
import asyncio
import copy
import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, IO

from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
from aidmi_orchestrator.evaluator.base import (
    Evaluator,
    RunArtifacts,
    make_evaluator,
)
from aidmi_orchestrator.fixtures.base import Fixture
from aidmi_orchestrator.orchestrator import run_orchestrator, StrategyExecutionError
from aidmi_orchestrator.persistence import write_benchmark_result
from aidmi_orchestrator.run_id import make_run_id, slug
from aidmi_orchestrator.strategy.base import Strategy


def parse_strategy_spec(spec: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Return (registered strategy name, spec display name, config dict)."""
    if "strategy" not in spec:
        raise ValueError("strategy spec must include top-level 'strategy'")
    if "name" not in spec:
        raise ValueError("strategy spec must include top-level 'name'")
    registry = spec["strategy"]
    spec_name = spec["name"]
    if not isinstance(registry, str) or not isinstance(spec_name, str):
        raise ValueError("strategy spec 'strategy' and 'name' must be strings")
    if not registry.strip() or not spec_name.strip():
        raise ValueError("strategy spec 'strategy' and 'name' must be non-empty")
    return registry, spec_name.strip(), dict(spec.get("config", {}) or {})


class Benchmark:
    def __init__(
        self,
        fixture: Fixture,
        workspace: Path,
        staging_db_url: str,
        evaluators: list[Evaluator] | None = None,
    ):
        self.fixture = fixture
        self.workspace = workspace
        self.staging_db_url = staging_db_url
        self.evaluators = evaluators or self._default_evaluators()

    def _default_evaluators(self) -> list[Evaluator]:
        return [make_evaluator(name) for name in self.fixture.applicable_evaluators]

    async def run(
        self,
        strategy: Strategy,
        *,
        strategy_spec_name: str,
        rep_index: int = 0,
        run_id: str | None = None,
        trace_mirror: IO[str] | None = None,
    ) -> BenchmarkResult:
        run_id = run_id or make_run_id(strategy.name, self.fixture.name)
        started_at = datetime.utcnow()
        error: str | None = None
        artifacts: RunArtifacts | None = None
        try:
            artifacts = await run_orchestrator(
                self.fixture,
                strategy,
                run_id,
                self.workspace,
                self.staging_db_url,
                trace_mirror=trace_mirror,
            )
        except StrategyExecutionError as e:
            error = repr(e)
        except Exception as e:
            error = f"harness: {e!r}"

        completed_at = datetime.utcnow()
        wall_clock = (completed_at - started_at).total_seconds()

        metrics: dict[str, Any] = {}
        if artifacts is not None:
            for ev in self.evaluators:
                try:
                    if ev.applies_to(artifacts):
                        metrics.update(await asyncio.to_thread(ev.evaluate, artifacts))
                except Exception as e:
                    metrics[f"evaluator_error_{ev.name}"] = repr(e)

        strategy_result = (
            artifacts.strategy_result
            if artifacts is not None
            else StrategyResult(
                target_tables_written=[], self_reported_status="gave_up"
            )
        )

        result = BenchmarkResult(
            run_id=run_id,
            fixture_name=self.fixture.name,
            strategy_name=strategy.name,
            strategy_spec_name=strategy_spec_name,
            strategy_config=strategy.config.model_dump()
            if strategy.config is not None
            else {},
            rep_index=rep_index,
            started_at=started_at,
            completed_at=completed_at,
            wall_clock_seconds=wall_clock,
            strategy_result=strategy_result,
            metrics=metrics,
            error=error,
            source_schema=(artifacts.source_schema if artifacts is not None else ""),
            out_schema=(artifacts.out_schema if artifacts is not None else ""),
        )
        write_benchmark_result(self.workspace / "runs" / run_id, result)
        return result


def sweep_job_status(result: BenchmarkResult) -> str:
    if result.error:
        return "ERROR"
    status = result.strategy_result.self_reported_status
    if status == "errored":
        return "ERRORED"
    if status == "gave_up":
        return "GAVE_UP"
    if status == "partial":
        return "PARTIAL"
    if result.metrics.get("dbt_success") is False:
        return "FAIL"
    return "ok"


def resolve_model_refs(
    config: dict[str, Any], models: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    out = dict(config)
    for key, value in config.items():
        if key.endswith("_model") and isinstance(value, str):
            if value not in models:
                raise ValueError(
                    f"unknown model ref {value!r} in field {key!r}. Defined: {sorted(models)}"
                )
            out[key] = copy.deepcopy(models[value])
    return out


def expand_grid(
    grid: dict[str, Any],
) -> list[tuple[str, dict[str, Any], str, list[str] | None]]:
    """Expand a grid YAML dict into (registry_strategy, config, spec_name, cell_fixtures) tuples.

    List-valued top-level scalar config fields expand cartesian-wise; suffixes
    derived from varied keys distinguish each combo. Fields ending in `_model`
    accept string refs into the top-level `models:` block (lists of refs expand
    like any scalar list, then resolve to spec dicts). A cell-level `fixtures:`
    list restricts which sweep fixtures the cell runs on (None = all).
    """
    models = grid.get("models", {}) or {}
    out: list[tuple[str, dict, str, list[str] | None]] = []
    for cell in grid.get("cells", []):
        registry = cell["strategy"]
        base_name = cell.get("name") or registry
        cell_fixtures = cell.get("fixtures")
        cfg = cell.get("config", {})
        list_keys = [k for k, v in cfg.items() if isinstance(v, list)]
        if not list_keys:
            out.append(
                (
                    registry,
                    resolve_model_refs(dict(cfg), models),
                    base_name,
                    cell_fixtures,
                )
            )
            continue
        scalar_part = {k: v for k, v in cfg.items() if k not in list_keys}
        for combo in itertools.product(*(cfg[k] for k in list_keys)):
            expanded = dict(scalar_part)
            for k, v in zip(list_keys, combo):
                expanded[k] = v
            suffix = "".join(f"_{k}_{slug(v)}" for k, v in zip(list_keys, combo))
            out.append(
                (
                    registry,
                    resolve_model_refs(expanded, models),
                    f"{base_name}{suffix}",
                    cell_fixtures,
                )
            )
    return out

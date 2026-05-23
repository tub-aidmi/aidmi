"""Benchmark harness: run() / sweep() with grid expansion."""
from __future__ import annotations
import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, IO

from ulid import ULID

from aidmi_orchestrator.domain import BenchmarkResult, StrategyResult
from aidmi_orchestrator.evaluator.base import (
    Evaluator, RunArtifacts, make_evaluator,
)
from aidmi_orchestrator.fixtures.base import Fixture
from aidmi_orchestrator.orchestrator import run_orchestrator, StrategyExecutionError
from aidmi_orchestrator.persistence import write_benchmark_result
from aidmi_orchestrator.strategy.base import Strategy, make_strategy


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
        run_id: str | None = None,
        trace_mirror: IO[str] | None = None,
    ) -> BenchmarkResult:
        run_id = run_id or str(ULID())
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

        completed_at = datetime.utcnow()
        wall_clock = (completed_at - started_at).total_seconds()

        metrics: dict[str, Any] = {}
        if artifacts is not None:
            for ev in self.evaluators:
                if ev.applies_to(artifacts):
                    metrics.update(ev.evaluate(artifacts))

        strategy_result = (
            artifacts.strategy_result if artifacts is not None
            else StrategyResult(target_tables_written=[], self_reported_status="gave_up")
        )

        result = BenchmarkResult(
            run_id=run_id,
            fixture_name=self.fixture.name,
            strategy_name=strategy.name,
            strategy_config=strategy.config.model_dump() if strategy.config is not None else {},
            started_at=started_at,
            completed_at=completed_at,
            wall_clock_seconds=wall_clock,
            strategy_result=strategy_result,
            metrics=metrics,
            error=error,
            staging_raw_dataset=(
                artifacts.staging_raw_dataset if artifacts is not None else ""
            ),
            staging_out_dataset=(
                artifacts.staging_out_dataset if artifacts is not None else ""
            ),
        )
        write_benchmark_result(self.workspace / "runs" / run_id, result)
        return result

    async def sweep(
        self,
        cells: list[Strategy],
        runs_per_cell: int = 1,
        results_path: Path | None = None,
        trace_mirror: IO[str] | None = None,
    ) -> list[BenchmarkResult]:
        results: list[BenchmarkResult] = []
        if results_path is not None:
            results_path.parent.mkdir(parents=True, exist_ok=True)
            results_fh = open(results_path, "a", encoding="utf-8")
        else:
            results_fh = None
        try:
            for strategy in cells:
                for _ in range(runs_per_cell):
                    r = await self.run(strategy, trace_mirror=trace_mirror)
                    results.append(r)
                    if results_fh is not None:
                        results_fh.write(r.model_dump_json() + "\n")
                        results_fh.flush()
        finally:
            if results_fh is not None:
                results_fh.close()
        return results


def expand_grid(grid: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Expand a grid YAML dict into a list of (strategy_name, config_dict) cells.

    A cell with list-valued top-level scalar config fields expands cartesian.
    """
    out: list[tuple[str, dict]] = []
    for cell in grid.get("cells", []):
        name = cell["strategy"]
        cfg = cell.get("config", {})
        list_keys = [k for k, v in cfg.items() if isinstance(v, list)]
        if not list_keys:
            out.append((name, dict(cfg)))
            continue
        scalar_part = {k: v for k, v in cfg.items() if k not in list_keys}
        for combo in itertools.product(*(cfg[k] for k in list_keys)):
            expanded = dict(scalar_part)
            for k, v in zip(list_keys, combo):
                expanded[k] = v
            out.append((name, expanded))
    return out

"""Sweep execution: grid settings, job orchestration, progress bookkeeping."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from aidmi_orchestrator.benchmark import sweep_job_status
from aidmi_orchestrator.campaign import Campaign
from aidmi_orchestrator.domain import BenchmarkResult
from aidmi_orchestrator.persistence import record_run
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.provenance import attach_provenance
from aidmi_orchestrator.scheduler import (
    DEFAULT_EXCLUSIVE_PREFIXES,
    SweepJob,
    completed_keys,
    expand_jobs,
    filter_resumed,
    run_jobs,
)
from aidmi_orchestrator.strategy.base import make_strategy


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


@dataclass(frozen=True)
class SweepSettings:
    fixtures: list[str]
    runs_per_cell: int
    concurrency: int
    exclusive_prefixes: tuple[str, ...]
    per_model_exclusive: bool

    @classmethod
    def from_grid(
        cls,
        grid: dict[str, Any],
        *,
        fixture_override: str | None = None,
        runs_per_cell: int = 1,
        concurrency: int | None = None,
    ) -> SweepSettings:
        fixtures = (
            [fixture_override] if fixture_override else _as_list(grid.get("fixture"))
        )
        if not fixtures:
            raise ValueError("provide --fixture or a 'fixture' key in grid.yaml.")
        return cls(
            fixtures=fixtures,
            runs_per_cell=(
                runs_per_cell
                if runs_per_cell != 1
                else int(grid.get("runs_per_cell", 1))
            ),
            concurrency=concurrency or int(grid.get("concurrency", 3)),
            exclusive_prefixes=tuple(
                grid.get("exclusive_model_prefixes", list(DEFAULT_EXCLUSIVE_PREFIXES))
            ),
            per_model_exclusive=bool(grid.get("per_model_exclusive", False)),
        )


async def run_sweep(
    campaign: Campaign,
    settings: SweepSettings,
    cells: list[tuple[str, dict[str, Any], str, list[str] | None]],
    bench_for: Callable[[str], Any],
    *,
    workspace: Path,
    resume: bool = True,
    archive_dbt: bool = True,
    mirror: IO[str] | None = None,
) -> list[BenchmarkResult]:
    jobs = expand_jobs(cells, settings.fixtures, settings.runs_per_cell)
    benches = {fx: bench_for(fx) for fx in dict.fromkeys(j.fixture_name for j in jobs)}

    results_path = campaign.results_jsonl
    if resume:
        before = len(jobs)
        jobs = filter_resumed(jobs, completed_keys(results_path))
        skipped = before - len(jobs)
        if skipped:
            log_message(f"resume: skipping {skipped} completed runs", scope="sweep")
    elif results_path.exists():
        results_path.unlink()
        log_message("fresh sweep: cleared existing results.jsonl", scope="sweep")

    if not jobs:
        log_message("nothing to run (all jobs already completed)", scope="sweep")
        return []

    total = len(jobs)
    counter = {"done": 0, "next": 0}
    lock = asyncio.Lock()

    async def run_job(job: SweepJob):
        async with lock:
            counter["next"] += 1
            position = counter["next"]
            log_message(
                f"[{position}/{total}] starting {job.spec_name} ({job.registry_strategy}) "
                f"@ {job.fixture_name} rep{job.rep_index}",
                scope="sweep",
            )
        strategy = make_strategy(job.registry_strategy, job.config)
        result = await benches[job.fixture_name].run(
            strategy,
            strategy_spec_name=job.spec_name,
            rep_index=job.rep_index,
            trace_mirror=mirror,
        )
        workspace_run = workspace / "runs" / result.run_id
        cell_spec = {
            "name": job.spec_name,
            "strategy": job.registry_strategy,
            "config": job.config,
        }
        result = attach_provenance(
            result,
            campaign_id=campaign.id,
            strategy_spec_path=None,
            workspace_run_dir=workspace_run,
        )
        async with lock:
            record_run(
                campaign.path,
                result,
                workspace_run,
                cell_spec=cell_spec,
                archive_dbt=archive_dbt,
            )
            counter["done"] += 1
            status = sweep_job_status(result)
            log_message(
                f"[{counter['done']}/{total}] finished {job.spec_name} @ {job.fixture_name} "
                f"rep{job.rep_index}: {status} ({result.wall_clock_seconds:.0f}s, "
                f"run_id={result.run_id})",
                scope="sweep",
            )
        return result

    log_message(f"running {total} jobs", scope="sweep")
    return await run_jobs(
        jobs,
        run_job,
        concurrency=settings.concurrency,
        prefixes=settings.exclusive_prefixes,
        per_model_exclusive=settings.per_model_exclusive,
    )

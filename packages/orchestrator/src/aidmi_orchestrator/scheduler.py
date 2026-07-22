"""Sweep scheduler: exclusive-model grouping, bounded concurrency, resume bookkeeping."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aidmi_orchestrator.progress import log_message

DEFAULT_EXCLUSIVE_PREFIXES: tuple[str, ...] = ("ise-",)


@dataclass(frozen=True)
class SweepJob:
    registry_strategy: str
    config: dict[str, Any] = field(compare=False)
    spec_name: str
    fixture_name: str
    rep_index: int


def model_names_in_config(config: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for key, value in config.items():
        if key.endswith("_model") and isinstance(value, dict):
            model_name = value.get("model_name")
            if model_name:
                names.add(model_name)
    return names


def exclusive_models(
    config: dict[str, Any], prefixes: tuple[str, ...]
) -> frozenset[str]:
    return frozenset(n for n in model_names_in_config(config) if n.startswith(prefixes))


def expand_jobs(
    cells: list[tuple[str, dict[str, Any], str, list[str] | None]],
    fixtures: list[str],
    runs_per_cell: int,
) -> list[SweepJob]:
    sweep_fixtures: list[str] = []
    seen_fixtures: set[str] = set()
    for fixture in fixtures:
        if fixture not in seen_fixtures:
            seen_fixtures.add(fixture)
            sweep_fixtures.append(fixture)
    for _, _, _, cell_fixtures in cells:
        if cell_fixtures:
            for fixture in cell_fixtures:
                if fixture not in seen_fixtures:
                    seen_fixtures.add(fixture)
                    sweep_fixtures.append(fixture)

    jobs: list[SweepJob] = []
    for rep in range(runs_per_cell):
        for fixture in sweep_fixtures:
            for registry, config, spec_name, cell_fixtures in cells:
                if cell_fixtures is not None:
                    if fixture not in cell_fixtures:
                        continue
                elif fixture not in fixtures:
                    continue
                jobs.append(SweepJob(registry, config, spec_name, fixture, rep))
    return jobs


def group_jobs(
    jobs: list[SweepJob],
    prefixes: tuple[str, ...],
) -> tuple[list[list[SweepJob]], list[SweepJob]]:
    groups: dict[frozenset[str], list[SweepJob]] = {}
    passthrough: list[SweepJob] = []
    for job in jobs:
        key = exclusive_models(job.config, prefixes)
        if not key:
            passthrough.append(job)
            continue
        if len(key) > 1:
            log_message(
                f"WARNING: {job.spec_name} mixes exclusive models {sorted(key)} — "
                f"every role switch will reload the model",
                scope="scheduler",
            )
        groups.setdefault(key, []).append(job)
    ordered = [groups[k] for k in sorted(groups, key=lambda k: sorted(k))]
    return ordered, passthrough


def completed_keys(results_path: Path) -> set[tuple[str, str, int]]:
    done: set[tuple[str, str, int]] = set()
    if not results_path.exists():
        return done
    for line in results_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            log_message(
                f"WARNING: skipping malformed results line in {results_path}",
                scope="scheduler",
            )
            continue
        done.add(
            (
                row["strategy_spec_name"],
                row["fixture_name"],
                int(row.get("rep_index", 0)),
            )
        )
    return done


def filter_resumed(
    jobs: list[SweepJob],
    done: set[tuple[str, str, int]],
) -> list[SweepJob]:
    return [j for j in jobs if (j.spec_name, j.fixture_name, j.rep_index) not in done]


async def run_jobs(
    jobs: list[SweepJob],
    run_job: Callable[[SweepJob], Awaitable[Any]],
    *,
    concurrency: int,
    prefixes: tuple[str, ...] = DEFAULT_EXCLUSIVE_PREFIXES,
    per_model_exclusive: bool = False,
) -> list[Any]:
    semaphore = asyncio.Semaphore(concurrency)

    if per_model_exclusive:
        model_locks: dict[str, asyncio.Lock] = {}
        for job in jobs:
            for name in model_names_in_config(job.config):
                model_locks.setdefault(name, asyncio.Lock())

        async def per_model_guarded(job: SweepJob) -> Any:
            models = sorted(model_names_in_config(job.config))
            async with semaphore:
                for model in models:
                    await model_locks[model].acquire()
                try:
                    return await run_job(job)
                finally:
                    for model in reversed(models):
                        model_locks[model].release()

        return list(await asyncio.gather(*(per_model_guarded(j) for j in jobs)))

    async def guarded(job: SweepJob) -> Any:
        async with semaphore:
            return await run_job(job)

    exclusive_groups, passthrough = group_jobs(jobs, prefixes)

    async def run_group(group: list[SweepJob]) -> list[Any]:
        return list(await asyncio.gather(*(guarded(j) for j in group)))

    async def run_exclusive_sequence() -> list[Any]:
        out: list[Any] = []
        for group in exclusive_groups:
            out.extend(await run_group(group))
        return out

    sequence_results, passthrough_results = await asyncio.gather(
        run_exclusive_sequence(),
        run_group(passthrough),
    )
    return sequence_results + passthrough_results

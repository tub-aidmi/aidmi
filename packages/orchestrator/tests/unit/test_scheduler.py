"""Scheduler: job expansion, exclusive-model grouping, concurrency, resume."""

from __future__ import annotations

import asyncio
import json

from aidmi_orchestrator.scheduler import (
    SweepJob,
    completed_keys,
    exclusive_models,
    expand_jobs,
    filter_resumed,
    group_jobs,
    run_jobs,
)


def _job(
    spec_name: str, model: str = "academic/big", fixture: str = "fx", rep: int = 0
) -> SweepJob:
    return SweepJob(
        registry_strategy="s",
        config={"writer_model": {"provider": "litellm", "model_name": model}},
        spec_name=spec_name,
        fixture_name=fixture,
        rep_index=rep,
    )


def test_exclusive_models_filters_by_prefix() -> None:
    cfg = {
        "writer_model": {"provider": "litellm", "model_name": "ise-ollama/q"},
        "critic_model": {"provider": "litellm", "model_name": "academic/big"},
        "samples_per_table": 3,
    }
    assert exclusive_models(cfg, ("ise-",)) == frozenset({"ise-ollama/q"})


def test_expand_jobs_rep_fixture_cell_order() -> None:
    cells = [("s", {"x": 1}, "a", None), ("s", {"x": 2}, "b", None)]
    jobs = expand_jobs(cells, fixtures=["fx1", "fx2"], runs_per_cell=2)
    assert len(jobs) == 8
    assert [(j.spec_name, j.fixture_name, j.rep_index) for j in jobs] == [
        ("a", "fx1", 0),
        ("b", "fx1", 0),
        ("a", "fx2", 0),
        ("b", "fx2", 0),
        ("a", "fx1", 1),
        ("b", "fx1", 1),
        ("a", "fx2", 1),
        ("b", "fx2", 1),
    ]


def test_expand_jobs_respects_cell_fixture_restrictions() -> None:
    cells = [("s", {"x": 1}, "a", None), ("s", {"x": 2}, "b", ["only_fx2"])]
    jobs = expand_jobs(cells, fixtures=["fx1", "fx2"], runs_per_cell=2)
    a_jobs = [j for j in jobs if j.spec_name == "a"]
    b_jobs = [j for j in jobs if j.spec_name == "b"]
    assert len(a_jobs) == 4  # 2 fixtures x 2 reps
    assert len(b_jobs) == 2  # restricted to only_fx2, 2 reps
    assert {j.fixture_name for j in b_jobs} == {"only_fx2"}
    assert {j.rep_index for j in a_jobs} == {0, 1}
    assert [(j.spec_name, j.fixture_name, j.rep_index) for j in jobs] == [
        ("a", "fx1", 0),
        ("a", "fx2", 0),
        ("b", "only_fx2", 0),
        ("a", "fx1", 1),
        ("a", "fx2", 1),
        ("b", "only_fx2", 1),
    ]


def test_group_jobs_splits_exclusive_and_passthrough() -> None:
    jobs = [
        _job("ise_a", model="ise-ollama/a"),
        _job("ise_b", model="ise-ollama/b"),
        _job("pass_1", model="academic/big"),
        _job("ise_a2", model="ise-ollama/a"),
    ]
    exclusive_groups, passthrough = group_jobs(jobs, ("ise-",))
    assert len(exclusive_groups) == 2
    assert {j.spec_name for j in passthrough} == {"pass_1"}
    sizes = sorted(len(g) for g in exclusive_groups)
    assert sizes == [1, 2]


def test_completed_keys_and_filter(tmp_path) -> None:
    results = tmp_path / "results.jsonl"
    rows = [
        {"strategy_spec_name": "a", "fixture_name": "fx", "rep_index": 0},
        {"strategy_spec_name": "a", "fixture_name": "fx", "rep_index": 1},
    ]
    results.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    done = completed_keys(results)
    jobs = [_job("a", rep=0), _job("a", rep=1), _job("a", rep=2), _job("b", rep=0)]
    remaining = filter_resumed(jobs, done)
    assert {(j.spec_name, j.rep_index) for j in remaining} == {("a", 2), ("b", 0)}


def test_completed_keys_missing_file(tmp_path) -> None:
    assert completed_keys(tmp_path / "absent.jsonl") == set()


def test_completed_keys_skips_malformed_lines(tmp_path) -> None:
    results = tmp_path / "results.jsonl"
    results.write_text(
        json.dumps({"strategy_spec_name": "a", "fixture_name": "fx", "rep_index": 0})
        + "\n"
        + '{"strategy_spec_name": "b", "fixture',
        encoding="utf-8",
    )
    done = completed_keys(results)
    assert done == {("a", "fx", 0)}


def test_run_jobs_exclusive_groups_never_overlap() -> None:
    jobs = [
        _job("a1", model="ise-ollama/a"),
        _job("a2", model="ise-ollama/a"),
        _job("b1", model="ise-ollama/b"),
        _job("p1", model="academic/big"),
    ]
    active_models: set[str] = set()
    overlaps: list[frozenset] = []

    async def run_job(job: SweepJob) -> str:
        key = exclusive_models(job.config, ("ise-",))
        if key:
            active_exclusive = {m for m in active_models if m.startswith("ise-")}
            if active_exclusive - set(key):
                overlaps.append(frozenset(active_exclusive | set(key)))
        active_models.update(key)
        await asyncio.sleep(0.02)
        active_models.difference_update(key)
        return job.spec_name

    results = asyncio.run(run_jobs(jobs, run_job, concurrency=4, prefixes=("ise-",)))
    assert sorted(results) == ["a1", "a2", "b1", "p1"]
    assert overlaps == []


def test_run_jobs_respects_concurrency_cap() -> None:
    jobs = [_job(f"p{i}") for i in range(6)]
    active = {"n": 0, "max": 0}

    async def run_job(job: SweepJob) -> None:
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.01)
        active["n"] -= 1

    asyncio.run(run_jobs(jobs, run_job, concurrency=2, prefixes=("ise-",)))
    assert active["max"] <= 2


def test_run_jobs_per_model_exclusive_three_models_parallel() -> None:
    jobs = [
        _job("a", model="model/a"),
        _job("b", model="model/b"),
        _job("c", model="model/c"),
    ]
    active = {"n": 0, "max": 0}

    async def run_job(job: SweepJob) -> None:
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.02)
        active["n"] -= 1

    asyncio.run(
        run_jobs(jobs, run_job, concurrency=3, per_model_exclusive=True),
    )
    assert active["max"] == 3


def test_run_jobs_per_model_exclusive_same_model_serial() -> None:
    jobs = [_job(f"j{i}", model="model/same") for i in range(3)]
    active_models: dict[str, int] = {}
    overlaps: list[str] = []

    async def run_job(job: SweepJob) -> str:
        model = "model/same"
        if active_models.get(model, 0) > 0:
            overlaps.append(job.spec_name)
        active_models[model] = active_models.get(model, 0) + 1
        await asyncio.sleep(0.02)
        active_models[model] -= 1
        return job.spec_name

    asyncio.run(
        run_jobs(jobs, run_job, concurrency=3, per_model_exclusive=True),
    )
    assert overlaps == []


def test_run_jobs_per_model_exclusive_global_cap() -> None:
    jobs = [_job(f"j{i}", model=f"model/{i}") for i in range(6)]
    active = {"n": 0, "max": 0}

    async def run_job(job: SweepJob) -> None:
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.01)
        active["n"] -= 1

    asyncio.run(
        run_jobs(jobs, run_job, concurrency=3, per_model_exclusive=True),
    )
    assert active["max"] <= 3

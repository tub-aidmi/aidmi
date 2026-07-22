"""Typer CLI: run, sweep, campaign, apply-dbt, evaluate, report."""

from __future__ import annotations
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Annotated
from urllib.parse import quote_plus

import typer
import yaml
from dotenv import load_dotenv

load_dotenv(override=True)

import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import (
    Benchmark,
    expand_grid,
    parse_strategy_spec,
    sweep_job_status,
)
from aidmi_orchestrator.campaign import (
    Campaign,
    DEFAULT_BENCHMARKS_ROOT,
    resolve_campaign,
)
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.persistence import record_run
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.provenance import file_sha256, make_run_provenance
from aidmi_orchestrator.repro import apply_recorded_run, evaluate_recorded_run
from aidmi_orchestrator.strategy.base import make_strategy

app = typer.Typer(add_completion=False, help="aidmi orchestrator runner")


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def staging_db_url_from_env() -> str | None:
    direct = os.environ.get("AIDMI_STAGING_DB_URL")
    if direct:
        return direct
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    database = os.environ.get("POSTGRES_DB")
    if user is None or password is None or database is None:
        return None
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    u = quote_plus(user, safe="")
    p = quote_plus(password, safe="")
    return f"postgresql://{u}:{p}@{host}:{port}/{database}"


def _require_staging_url() -> str:
    url = staging_db_url_from_env()
    if not url:
        raise typer.BadParameter(
            "set AIDMI_STAGING_DB_URL, or POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB "
            "(optional POSTGRES_HOST default localhost, POSTGRES_PORT default 5432)."
        )
    return url


def _spec_repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(path.resolve())


def _attach_provenance(
    result,
    *,
    campaign_id: str,
    strategy_spec_path: Path | None,
    workspace_run_dir: Path,
):
    spec_rel = _spec_repo_relative(strategy_spec_path) if strategy_spec_path else None
    spec_hash = (
        file_sha256(strategy_spec_path)
        if strategy_spec_path and strategy_spec_path.is_file()
        else None
    )
    prov = make_run_provenance(
        campaign_id=campaign_id,
        strategy_spec_path=spec_rel,
        strategy_spec_sha256=spec_hash,
        workspace_run_dir=workspace_run_dir,
    )
    return result.model_copy(update={"provenance": prov})


campaign_app = typer.Typer(help="Campaign management")
app.add_typer(campaign_app, name="campaign")


@campaign_app.command("new")
def campaign_new(
    label: Annotated[str | None, typer.Argument(help="optional human label")] = None,
    benchmarks_root: Annotated[
        Path, typer.Option(help="benchmarks root directory")
    ] = DEFAULT_BENCHMARKS_ROOT,
):
    """Create a new campaign directory."""
    camp = Campaign.create(label=label, root=benchmarks_root)
    typer.echo(f"created campaign {camp.id} -> {camp.path}")
    if label:
        typer.echo(f"label: {label}")


@app.command()
def run(
    fixture: Annotated[str, typer.Option(help="registered fixture name")],
    strategy_spec: Annotated[Path, typer.Option(help="path to a strategy YAML")],
    campaign: Annotated[str, typer.Option(help="campaign id or path")],
    run_id: Annotated[
        str | None, typer.Option(help="optional run id (auto-generated slug otherwise)")
    ] = None,
    workspace: Annotated[
        Path, typer.Option(help="transient workspace directory")
    ] = Path("./aidmi_workspace"),
    benchmarks_root: Annotated[
        Path, typer.Option(help="benchmarks root directory")
    ] = DEFAULT_BENCHMARKS_ROOT,
    archive_dbt: Annotated[
        bool,
        typer.Option("--archive-dbt/--no-archive-dbt", help="copy dbt into run bundle"),
    ] = True,
    verbose: Annotated[
        bool, typer.Option("-v", "--verbose", help="stream trace JSONL lines to stderr")
    ] = False,
):
    """Run one orchestrator pass and record into a campaign."""
    spec = yaml.safe_load(strategy_spec.read_text(encoding="utf-8"))
    try:
        registry, spec_name, config = parse_strategy_spec(spec)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None

    camp = resolve_campaign(campaign, benchmarks_root)
    camp.ensure_layout()

    log_message(
        f"run {spec_name} ({registry}) on {fixture} -> campaign {camp.id}",
        scope="run",
    )

    strategy = make_strategy(registry, config)
    fx = get_fixture(fixture)
    bench = Benchmark(fx, workspace, _require_staging_url())
    result = asyncio.run(
        bench.run(
            strategy,
            strategy_spec_name=spec_name,
            run_id=run_id,
            trace_mirror=sys.stderr if verbose else None,
        ),
    )
    workspace_run = workspace / "runs" / result.run_id
    result = _attach_provenance(
        result,
        campaign_id=camp.id,
        strategy_spec_path=strategy_spec.resolve(),
        workspace_run_dir=workspace_run,
    )
    bundle = record_run(
        camp.path,
        result,
        workspace_run,
        strategy_spec_path=strategy_spec.resolve(),
        archive_dbt=archive_dbt,
    )
    typer.echo(result.model_dump_json(indent=2))
    typer.echo(f"recorded -> {bundle}", err=True)


@app.command()
def sweep(
    campaign: Annotated[str, typer.Option(help="campaign id or path")],
    fixture: Annotated[str | None, typer.Option(help="single fixture override")] = None,
    runs_per_cell: Annotated[
        int, typer.Option(help="repetitions per cell (overrides grid YAML)")
    ] = 1,
    workspace: Annotated[
        Path, typer.Option(help="transient workspace directory")
    ] = Path("./aidmi_workspace"),
    benchmarks_root: Annotated[
        Path, typer.Option(help="benchmarks root directory")
    ] = DEFAULT_BENCHMARKS_ROOT,
    concurrency: Annotated[
        int | None, typer.Option(help="parallel runs (overrides grid YAML)")
    ] = None,
    resume: Annotated[
        bool,
        typer.Option(
            "--resume/--no-resume", help="skip completed (spec, fixture, rep) tuples"
        ),
    ] = True,
    archive_dbt: Annotated[
        bool,
        typer.Option(
            "--archive-dbt/--no-archive-dbt", help="copy dbt into run bundles"
        ),
    ] = True,
    verbose: Annotated[
        bool, typer.Option("-v", "--verbose", help="stream trace (concurrency 1 only)")
    ] = False,
):
    """Sweep cells from the campaign's grid.yaml."""
    from aidmi_orchestrator.scheduler import (
        DEFAULT_EXCLUSIVE_PREFIXES,
        SweepJob,
        completed_keys,
        expand_jobs,
        filter_resumed,
        run_jobs,
    )

    camp = resolve_campaign(campaign, benchmarks_root)
    camp.ensure_layout()

    if not camp.grid_yaml.is_file():
        raise typer.BadParameter(f"no grid.yaml at {camp.grid_yaml}")

    grid_data = yaml.safe_load(camp.grid_yaml.read_text(encoding="utf-8"))
    fixtures = [fixture] if fixture else _as_list(grid_data.get("fixture"))
    if not fixtures:
        raise typer.BadParameter("provide --fixture or a 'fixture' key in grid.yaml.")

    resolved_runs = (
        runs_per_cell if runs_per_cell != 1 else int(grid_data.get("runs_per_cell", 1))
    )
    resolved_concurrency = concurrency or int(grid_data.get("concurrency", 3))
    prefixes = tuple(
        grid_data.get("exclusive_model_prefixes", list(DEFAULT_EXCLUSIVE_PREFIXES))
    )
    per_model_exclusive = bool(grid_data.get("per_model_exclusive", False))

    cells = expand_grid(grid_data)
    jobs = expand_jobs(cells, fixtures, resolved_runs)
    cell_names = sorted({job.spec_name for job in jobs})

    log_message(
        f"sweep campaign {camp.id}: {len(cells)} cells, {len(jobs)} jobs, "
        f"fixtures={fixtures}, runs_per_cell={resolved_runs}, concurrency={resolved_concurrency}",
        scope="sweep",
    )
    log_message(f"cells: {', '.join(cell_names)}", scope="sweep")

    staging_url = _require_staging_url()
    benches = {
        fx: Benchmark(get_fixture(fx), workspace, staging_url) for fx in fixtures
    }
    for job in jobs:
        if job.fixture_name not in benches:
            benches[job.fixture_name] = Benchmark(
                get_fixture(job.fixture_name), workspace, staging_url
            )

    results_path = camp.results_jsonl
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
        return

    mirror = sys.stderr if (verbose and resolved_concurrency == 1) else None
    if verbose and resolved_concurrency > 1:
        log_message(
            "verbose trace JSONL mirroring disabled for concurrency > 1", scope="sweep"
        )

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
        result = _attach_provenance(
            result,
            campaign_id=camp.id,
            strategy_spec_path=None,
            workspace_run_dir=workspace_run,
        )
        async with lock:
            record_run(
                camp.path,
                result,
                workspace_run,
                cell_spec=cell_spec,
                archive_dbt=archive_dbt,
            )
            counter["done"] += 1
            status = sweep_job_status(result)
            log_message(
                f"[{counter['done']}/{total}] finished {job.spec_name} @ {job.fixture_name} "
                f"rep{job.rep_index}: {status} ({result.wall_clock_seconds:.0f}s, run_id={result.run_id})",
                scope="sweep",
            )
        return result

    log_message(f"running {total} jobs", scope="sweep")
    results = asyncio.run(
        run_jobs(
            jobs,
            run_job,
            concurrency=resolved_concurrency,
            prefixes=prefixes,
            per_model_exclusive=per_model_exclusive,
        )
    )
    failed = sum(1 for r in results if r.error)
    log_message(
        f"wrote {len(results)} runs to {camp.path} ({failed} with errors)",
        scope="sweep",
    )


@app.command("apply-dbt")
def apply_dbt_cmd(
    run_id: Annotated[str, typer.Option("--run-id", help="run id to apply")],
    campaign: Annotated[str, typer.Option(help="campaign id or path")],
    rep_index: Annotated[int, typer.Option(help="rep index for repeated cells")] = 0,
    benchmarks_root: Annotated[
        Path, typer.Option(help="benchmarks root directory")
    ] = DEFAULT_BENCHMARKS_ROOT,
):
    """Re-apply archived dbt SQL from a recorded run (transform only, no LLM)."""
    camp = resolve_campaign(campaign, benchmarks_root)

    result, transform_result = apply_recorded_run(
        camp,
        run_id,
        _require_staging_url(),
        rep_index=rep_index,
    )
    typer.echo(transform_result.model_dump_json(indent=2))
    typer.echo(
        f"applied dbt for {run_id} -> schema {result.out_schema}",
        err=True,
    )


@app.command()
def evaluate(
    run_id: Annotated[str, typer.Option("--run-id", help="run id to evaluate")],
    campaign: Annotated[str, typer.Option(help="campaign id or path")],
    rep_index: Annotated[int, typer.Option(help="rep index for repeated cells")] = 0,
    benchmarks_root: Annotated[
        Path, typer.Option(help="benchmarks root directory")
    ] = DEFAULT_BENCHMARKS_ROOT,
):
    """Re-run evaluators against a recorded run's out_schema (no LLM)."""
    camp = resolve_campaign(campaign, benchmarks_root)

    metrics = asyncio.run(
        evaluate_recorded_run(
            camp,
            run_id,
            _require_staging_url(),
            rep_index=rep_index,
        )
    )
    typer.echo(json.dumps(metrics, indent=2))


@app.command()
def report(
    results: Annotated[
        list[Path], typer.Argument(help="campaign dirs or results.jsonl files")
    ],
    out: Annotated[Path, typer.Option(help="report output directory")] = Path(
        "./report"
    ),
    exclude: Annotated[
        list[str] | None,
        typer.Option(
            help="strategy (cell) to drop, on top of EXCLUDED_STRATEGIES; repeatable"
        ),
    ] = None,
):
    """Render benchmark campaign(s) into an SVG figure gallery."""
    from aidmi_orchestrator.report.data import campaign_labels, load_records
    from aidmi_orchestrator.report.driver import build_report

    records = load_records(results)
    if not records:
        raise typer.BadParameter("no result rows found in the given paths")
    labels = campaign_labels(results)
    out.mkdir(parents=True, exist_ok=True)
    written = build_report(records, out, exclude=set(exclude or []), labels=labels)
    typer.echo(f"report over {len(records)} runs -> {out}: {len(written)} files")


if __name__ == "__main__":
    app()

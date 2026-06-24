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

from aidmi_orchestrator.benchmark import Benchmark, expand_grid, parse_strategy_spec, sweep_job_status
from aidmi_orchestrator.campaign import (
    Campaign,
    DEFAULT_BENCHMARKS_ROOT,
    resolve_active_campaign,
    resolve_campaign,
    results_jsonl_for_campaign,
)
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.persistence import record_run
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
    spec_hash = file_sha256(strategy_spec_path) if strategy_spec_path and strategy_spec_path.is_file() else None
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
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
):
    """Create a new campaign and set it as active."""
    from aidmi_orchestrator.campaign import write_active_campaign

    camp = Campaign.create(label=label, root=benchmarks_root)
    write_active_campaign(camp.id, benchmarks_root)
    typer.echo(f"created campaign {camp.id} -> {camp.path}")
    if label:
        typer.echo(f"label: {label}")


@campaign_app.command("use")
def campaign_use(
    campaign_id: Annotated[str, typer.Argument(help="campaign id or path")],
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
):
    """Set the active campaign."""
    from aidmi_orchestrator.campaign import write_active_campaign

    camp = resolve_campaign(campaign_id, benchmarks_root, auto_create=False)
    write_active_campaign(camp.id, benchmarks_root)
    typer.echo(f"active campaign: {camp.id} ({camp.path})")


@campaign_app.command("show")
def campaign_show(
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
):
    """Show the active campaign."""
    camp = resolve_active_campaign(benchmarks_root, auto_create=False)
    typer.echo(f"{camp.id}\t{camp.path}")
    if camp.campaign_yaml.is_file():
        meta = yaml.safe_load(camp.campaign_yaml.read_text(encoding="utf-8"))
        if meta.get("label"):
            typer.echo(f"label: {meta['label']}")


@app.command()
def run(
    fixture: Annotated[str, typer.Option(help="registered fixture name")],
    strategy_spec: Annotated[Path, typer.Option(help="path to a strategy YAML")],
    campaign: Annotated[str | None, typer.Option(help="campaign id or path (default: active, auto-create)")] = None,
    run_id: Annotated[str | None, typer.Option(help="optional run id (auto-generated slug otherwise)")] = None,
    workspace: Annotated[Path, typer.Option(help="transient workspace directory")] = Path("./aidmi_workspace"),
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
    archive_dbt: Annotated[bool, typer.Option("--archive-dbt/--no-archive-dbt", help="copy dbt into run bundle")] = True,
    verbose: Annotated[bool, typer.Option("-v", "--verbose", help="stream trace JSONL lines to stderr")] = False,
):
    """Run one orchestrator pass and record into a campaign."""
    spec = yaml.safe_load(strategy_spec.read_text(encoding="utf-8"))
    try:
        registry, spec_name, config = parse_strategy_spec(spec)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None

    if campaign is not None:
        camp = resolve_campaign(campaign, benchmarks_root, auto_create=False)
    else:
        camp = resolve_active_campaign(benchmarks_root, auto_create=True)
    camp.ensure_layout()

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
    campaign: Annotated[str | None, typer.Option(help="campaign id or path (default: active)")] = None,
    fixture: Annotated[str | None, typer.Option(help="single fixture override")] = None,
    runs_per_cell: Annotated[int, typer.Option(help="repetitions per cell (overrides grid YAML)")] = 1,
    workspace: Annotated[Path, typer.Option(help="transient workspace directory")] = Path("./aidmi_workspace"),
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
    concurrency: Annotated[int | None, typer.Option(help="parallel runs (overrides grid YAML)")] = None,
    resume: Annotated[bool, typer.Option("--resume/--no-resume", help="skip completed (spec, fixture, rep) tuples")] = True,
    archive_dbt: Annotated[bool, typer.Option("--archive-dbt/--no-archive-dbt", help="copy dbt into run bundles")] = True,
    verbose: Annotated[bool, typer.Option("-v", "--verbose", help="stream trace (concurrency 1 only)")] = False,
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

    if campaign is not None:
        camp = resolve_campaign(campaign, benchmarks_root, auto_create=False)
    else:
        camp = resolve_active_campaign(benchmarks_root, auto_create=False)
    camp.ensure_layout()

    if not camp.grid_yaml.is_file():
        raise typer.BadParameter(f"no grid.yaml at {camp.grid_yaml}")

    grid_data = yaml.safe_load(camp.grid_yaml.read_text(encoding="utf-8"))
    fixtures = [fixture] if fixture else _as_list(grid_data.get("fixture"))
    if not fixtures:
        raise typer.BadParameter("provide --fixture or a 'fixture' key in grid.yaml.")

    resolved_runs = runs_per_cell if runs_per_cell != 1 else int(grid_data.get("runs_per_cell", 1))
    resolved_concurrency = concurrency or int(grid_data.get("concurrency", 3))
    prefixes = tuple(grid_data.get("exclusive_model_prefixes", list(DEFAULT_EXCLUSIVE_PREFIXES)))
    per_model_exclusive = bool(grid_data.get("per_model_exclusive", False))

    cells = expand_grid(grid_data)
    jobs = expand_jobs(cells, fixtures, resolved_runs)

    staging_url = _require_staging_url()
    benches = {fx: Benchmark(get_fixture(fx), workspace, staging_url) for fx in fixtures}
    for job in jobs:
        if job.fixture_name not in benches:
            benches[job.fixture_name] = Benchmark(get_fixture(job.fixture_name), workspace, staging_url)

    results_path = camp.results_jsonl
    if resume:
        before = len(jobs)
        jobs = filter_resumed(jobs, completed_keys(results_path))
        if before - len(jobs):
            typer.echo(f"resume: skipping {before - len(jobs)} completed runs")
    elif results_path.exists():
        results_path.unlink()

    mirror = sys.stderr if (verbose and resolved_concurrency == 1) else None
    if verbose and resolved_concurrency > 1:
        typer.echo("verbose trace mirroring disabled for concurrency > 1", err=True)

    total = len(jobs)
    counter = {"done": 0}
    lock = asyncio.Lock()

    async def run_job(job: SweepJob):
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
            typer.echo(
                f"[{counter['done']}/{total}] {job.spec_name} @ {job.fixture_name} "
                f"rep{job.rep_index}: {status} ({result.wall_clock_seconds:.0f}s)"
            )
        return result

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
    typer.echo(f"wrote {len(results)} runs to {camp.path} ({failed} with errors)")


@app.command("apply-dbt")
def apply_dbt_cmd(
    run_id: Annotated[str, typer.Option("--run-id", help="run id to apply")],
    campaign: Annotated[str | None, typer.Option(help="campaign id or path (default: active)")] = None,
    rep_index: Annotated[int, typer.Option(help="rep index for repeated cells")] = 0,
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
):
    """Re-apply archived dbt SQL from a recorded run (transform only, no LLM)."""
    if campaign is not None:
        camp = resolve_campaign(campaign, benchmarks_root, auto_create=False)
    else:
        camp = resolve_active_campaign(benchmarks_root, auto_create=False)

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
    campaign: Annotated[str | None, typer.Option(help="campaign id or path (default: active)")] = None,
    rep_index: Annotated[int, typer.Option(help="rep index for repeated cells")] = 0,
    benchmarks_root: Annotated[Path, typer.Option(help="benchmarks root directory")] = DEFAULT_BENCHMARKS_ROOT,
):
    """Re-run evaluators against a recorded run's out_schema (no LLM)."""
    if campaign is not None:
        camp = resolve_campaign(campaign, benchmarks_root, auto_create=False)
    else:
        camp = resolve_active_campaign(benchmarks_root, auto_create=False)

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
    results: Annotated[list[Path], typer.Argument(help="campaign dirs or results.jsonl files")],
    out: Annotated[Path, typer.Option(help="report output directory")] = Path("./report"),
    matrix_metric: Annotated[str, typer.Option(help="metric for the strategy × model matrix")] = "target_columns_covered",
    metrics: Annotated[str | None, typer.Option(help="comma-separated headline metric override")] = None,
    no_plots: Annotated[bool, typer.Option("--no-plots", help="skip SVG heatmaps")] = False,
):
    """Aggregate campaign results into markdown/CSV tables and SVG heatmaps."""
    import aidmi_orchestrator.report  # noqa: F401
    from aidmi_orchestrator.report.aggregate import aggregate, build_rep_series, load_results
    from aidmi_orchestrator.report.catalog import build_report_plan
    from aidmi_orchestrator.report.render.markdown import render_markdown, render_matrix
    from aidmi_orchestrator.report.render.plots import write_plots
    from aidmi_orchestrator.report.render.tables import write_tables

    rows = load_results(results)
    if not rows:
        raise typer.BadParameter("no result rows found in the given paths")
    cells = aggregate(rows)
    series = build_rep_series(rows)
    plan = build_report_plan()
    headline = [m.strip() for m in metrics.split(",")] if metrics else plan.headline_metrics

    out.mkdir(parents=True, exist_ok=True)
    md = render_markdown(cells, headline) + "\n" + render_matrix(cells, matrix_metric)
    (out / "summary.md").write_text(md, encoding="utf-8")
    write_tables(cells, out)
    written = ["summary.md", "cells.csv", "summary.csv"]
    if not no_plots:
        try:
            artifacts = write_plots(cells, series, rows, out / "plots", plan)
        except RuntimeError as e:
            raise typer.BadParameter(str(e)) from None
        n_svg = sum(1 for p in artifacts if p.suffix == ".svg")
        n_csv = sum(1 for p in artifacts if p.suffix == ".csv")
        written.append(f"plots/ ({n_svg} SVGs, {n_csv} CSVs)")
    typer.echo(f"report over {len(rows)} runs / {len(cells)} cells -> {out}: {', '.join(written)}")


if __name__ == "__main__":
    app()

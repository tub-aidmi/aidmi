"""Typer CLI: `aidmi-orchestrator run` and `... sweep`."""
from __future__ import annotations
import asyncio
import os
import sys
from pathlib import Path
from typing import Annotated
from urllib.parse import quote_plus

import typer
import yaml
from dotenv import load_dotenv

load_dotenv(override=True)

# Trigger all built-in registrations.
import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import Benchmark, expand_grid, parse_strategy_spec
from aidmi_orchestrator.persistence import archive_run_dbt
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy

app = typer.Typer(add_completion=False, help="aidmi orchestrator runner")


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def staging_db_url_from_env() -> str | None:
    """Resolve staging DB URL: explicit `AIDMI_STAGING_DB_URL`, else build from POSTGRES_*."""
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


@app.command()
def run(
    fixture: Annotated[str, typer.Option(help="registered fixture name")],
    strategy_spec: Annotated[Path, typer.Option(help="path to a strategy YAML")],
    run_id: Annotated[str | None, typer.Option(help="optional run id (auto-generated slug otherwise)")] = None,
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
    verbose: Annotated[bool, typer.Option("-v", "--verbose", help="stream trace JSONL lines to stderr as they are recorded")] = False,
):
    """Run one orchestrator pass against a fixture."""
    spec = yaml.safe_load(strategy_spec.read_text(encoding="utf-8"))
    try:
        registry, spec_name, config = parse_strategy_spec(spec)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None
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
    typer.echo(result.model_dump_json(indent=2))


@app.command()
def sweep(
    grid: Annotated[Path, typer.Option(help="path to a grid YAML")],
    out: Annotated[Path, typer.Option(help="results directory")],
    fixture: Annotated[str | None, typer.Option(help="single fixture override (grid YAML 'fixture' may be a list)")] = None,
    runs_per_cell: Annotated[int, typer.Option(help="repetitions per cell (overrides grid YAML)")] = 1,
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
    concurrency: Annotated[int | None, typer.Option(help="parallel runs (overrides grid YAML 'concurrency', default 3)")] = None,
    resume: Annotated[bool, typer.Option("--resume/--no-resume", help="skip (spec, fixture, rep) tuples already in results.jsonl")] = True,
    archive_dbt: Annotated[bool, typer.Option("--archive-dbt/--no-archive-dbt", help="copy each run's dbt source into out/dbt/<run_id>/")] = True,
    verbose: Annotated[bool, typer.Option("-v", "--verbose", help="stream trace JSONL to stderr (concurrency 1 only)")] = False,
):
    """Sweep (strategy, config) cells across fixtures with model-major scheduling."""
    from aidmi_orchestrator.scheduler import (
        DEFAULT_EXCLUSIVE_PREFIXES, SweepJob, completed_keys, expand_jobs,
        filter_resumed, run_jobs,
    )

    grid_data = yaml.safe_load(grid.read_text(encoding="utf-8"))
    fixtures = [fixture] if fixture else _as_list(grid_data.get("fixture"))
    if not fixtures:
        raise typer.BadParameter("provide --fixture or a 'fixture' key (string or list) in the grid YAML.")
    resolved_runs = (
        runs_per_cell if runs_per_cell != 1 else int(grid_data.get("runs_per_cell", 1))
    )
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

    out.mkdir(parents=True, exist_ok=True)
    results_path = out / "results.jsonl"
    if resume:
        before = len(jobs)
        jobs = filter_resumed(jobs, completed_keys(results_path))
        if before - len(jobs):
            typer.echo(f"resume: skipping {before - len(jobs)} completed runs")
    elif results_path.exists():
        results_path.unlink()
    (out / "sweep_config.yaml").write_text(grid.read_text(), encoding="utf-8")

    mirror = sys.stderr if (verbose and resolved_concurrency == 1) else None
    if verbose and resolved_concurrency > 1:
        typer.echo("verbose trace mirroring disabled for concurrency > 1", err=True)

    total = len(jobs)
    counter = {"done": 0, "archived": 0}
    lock = asyncio.Lock()
    fh = open(results_path, "a", encoding="utf-8")

    async def run_job(job: SweepJob):
        strategy = make_strategy(job.registry_strategy, job.config)
        result = await benches[job.fixture_name].run(
            strategy, strategy_spec_name=job.spec_name,
            rep_index=job.rep_index, trace_mirror=mirror,
        )
        async with lock:
            fh.write(result.model_dump_json() + "\n")
            fh.flush()
            if archive_dbt and archive_run_dbt(
                workspace / "runs" / result.run_id,
                out / "dbt" / result.run_id,
            ):
                counter["archived"] += 1
            counter["done"] += 1
            status = "ERROR" if result.error else "ok"
            typer.echo(
                f"[{counter['done']}/{total}] {job.spec_name} @ {job.fixture_name} "
                f"rep{job.rep_index}: {status} ({result.wall_clock_seconds:.0f}s)"
            )
        return result

    try:
        results = asyncio.run(
            run_jobs(
                jobs,
                run_job,
                concurrency=resolved_concurrency,
                prefixes=prefixes,
                per_model_exclusive=per_model_exclusive,
            )
        )
    finally:
        fh.close()
    failed = sum(1 for r in results if r.error)
    msg = f"wrote {len(results)} result rows to {results_path} ({failed} with errors)"
    if archive_dbt:
        msg += f"; archived dbt for {counter['archived']} runs under {out / 'dbt'}"
    typer.echo(msg)


@app.command("archive-dbt")
def archive_dbt_cmd(
    out: Annotated[Path, typer.Option(help="sweep output directory containing results.jsonl")],
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
):
    """Backfill dbt source projects from workspace runs into out/dbt/<run_id>/."""
    import json

    results_path = out / "results.jsonl"
    if not results_path.is_file():
        raise typer.BadParameter(f"no results.jsonl at {results_path}")

    archived = 0
    skipped = 0
    missing = 0
    for line in results_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        run_id = json.loads(line)["run_id"]
        dest = out / "dbt" / run_id
        if (dest / "dbt_project").exists():
            skipped += 1
            continue
        if archive_run_dbt(workspace / "runs" / run_id, dest):
            archived += 1
        else:
            missing += 1
            typer.echo(f"missing dbt source for {run_id}", err=True)

    typer.echo(
        f"archive-dbt: {archived} copied, {skipped} already present, {missing} missing "
        f"-> {out / 'dbt'}"
    )


@app.command()
def report(
    results: Annotated[list[Path], typer.Argument(help="results.jsonl files or sweep output dirs")],
    out: Annotated[Path, typer.Option(help="report output directory")] = Path("./report"),
    matrix_metric: Annotated[str, typer.Option(help="metric for the strategy × model matrix")] = "target_columns_covered",
    metrics: Annotated[str | None, typer.Option(help="comma-separated headline metric override")] = None,
    no_plots: Annotated[bool, typer.Option("--no-plots", help="skip SVG heatmaps")] = False,
):
    """Aggregate sweep results into markdown/CSV tables and SVG heatmaps."""
    import aidmi_orchestrator.report  # noqa: F401 — register contributors
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

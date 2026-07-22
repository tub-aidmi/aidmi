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

import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401
import aidmi_orchestrator.strategy  # noqa: F401
from aidmi_orchestrator.benchmark import (
    Benchmark,
    expand_grid,
    parse_strategy_spec,
)
from aidmi_orchestrator.campaign import (
    DEFAULT_BENCHMARKS_ROOT,
    Campaign,
    resolve_campaign,
)
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.persistence import record_run
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.repro import apply_recorded_run, evaluate_recorded_run
from aidmi_orchestrator.strategy.base import make_strategy
from aidmi_orchestrator.sweep import attach_provenance

app = typer.Typer(add_completion=False, help="aidmi orchestrator runner")


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
    result = attach_provenance(
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
    from aidmi_orchestrator.scheduler import expand_jobs
    from aidmi_orchestrator.sweep import SweepSettings, run_sweep

    camp = resolve_campaign(campaign, benchmarks_root)
    camp.ensure_layout()

    if not camp.grid_yaml.is_file():
        raise typer.BadParameter(f"no grid.yaml at {camp.grid_yaml}")

    grid_data = yaml.safe_load(camp.grid_yaml.read_text(encoding="utf-8"))
    try:
        settings = SweepSettings.from_grid(
            grid_data,
            fixture_override=fixture,
            runs_per_cell=runs_per_cell,
            concurrency=concurrency,
        )
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None

    cells = expand_grid(grid_data)
    cell_names = sorted({name for _, _, name, _ in cells})
    job_count = len(expand_jobs(cells, settings.fixtures, settings.runs_per_cell))
    log_message(
        f"sweep campaign {camp.id}: {len(cells)} cells, {job_count} jobs, "
        f"fixtures={settings.fixtures}, runs_per_cell={settings.runs_per_cell}, "
        f"concurrency={settings.concurrency}",
        scope="sweep",
    )
    log_message(f"cells: {', '.join(cell_names)}", scope="sweep")

    staging_url = _require_staging_url()
    mirror = sys.stderr if (verbose and settings.concurrency == 1) else None
    if verbose and settings.concurrency > 1:
        log_message(
            "verbose trace JSONL mirroring disabled for concurrency > 1", scope="sweep"
        )

    results = asyncio.run(
        run_sweep(
            camp,
            settings,
            cells,
            lambda fx: Benchmark(get_fixture(fx), workspace, staging_url),
            workspace=workspace,
            resume=resume,
            archive_dbt=archive_dbt,
            mirror=mirror,
        )
    )
    if not results:
        return
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

"""Typer CLI: `aidmi-orchestrator run` and `... sweep`."""
from __future__ import annotations
import asyncio
import os
from pathlib import Path
from typing import Annotated
from urllib.parse import quote_plus

import typer
import yaml
from dotenv import load_dotenv

load_dotenv()

# Trigger all built-in registrations.
import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import Benchmark, expand_grid
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy

app = typer.Typer(add_completion=False, help="aidmi orchestrator runner")


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
    run_id: Annotated[str | None, typer.Option(help="optional run id (ULID auto-generated otherwise)")] = None,
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
):
    """Run one orchestrator pass against a fixture."""
    spec = yaml.safe_load(strategy_spec.read_text(encoding="utf-8"))
    strategy = make_strategy(spec["strategy"], spec.get("config", {}))
    fx = get_fixture(fixture)
    bench = Benchmark(fx, workspace, _require_staging_url())
    result = asyncio.run(bench.run(strategy, run_id=run_id))
    typer.echo(result.model_dump_json(indent=2))


@app.command()
def sweep(
    grid: Annotated[Path, typer.Option(help="path to a grid YAML")],
    out: Annotated[Path, typer.Option(help="results directory")],
    fixture: Annotated[str | None, typer.Option(help="registered fixture name (overrides grid YAML fixture key)")] = None,
    runs_per_cell: Annotated[int, typer.Option(help="repetitions per cell (overrides grid YAML runs_per_cell key)")] = 1,
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
):
    """Sweep multiple (strategy, config) cells across a fixture.

    Fixture and runs_per_cell can be defined in the grid YAML. CLI flags take
    precedence when explicitly provided; YAML values are used as defaults.
    """
    grid_data = yaml.safe_load(grid.read_text(encoding="utf-8"))
    resolved_fixture = fixture or grid_data.get("fixture")
    if not resolved_fixture:
        raise typer.BadParameter(
            "provide --fixture or add a 'fixture' key to the grid YAML."
        )
    resolved_runs_per_cell = (
        runs_per_cell if runs_per_cell != 1 else int(grid_data.get("runs_per_cell", runs_per_cell))
    )
    cells_specs = expand_grid(grid_data)
    cells = [make_strategy(name, cfg) for name, cfg in cells_specs]
    fx = get_fixture(resolved_fixture)
    bench = Benchmark(fx, workspace, _require_staging_url())
    out.mkdir(parents=True, exist_ok=True)
    (out / "sweep_config.yaml").write_text(grid.read_text(), encoding="utf-8")
    results = asyncio.run(bench.sweep(cells, runs_per_cell=resolved_runs_per_cell, results_path=out / "results.jsonl"))
    typer.echo(f"wrote {len(results)} result rows to {out / 'results.jsonl'}")


if __name__ == "__main__":
    app()

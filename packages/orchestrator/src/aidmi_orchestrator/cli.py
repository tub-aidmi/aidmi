"""Typer CLI: `aidmi-orchestrator run` and `... sweep`."""
from __future__ import annotations
import asyncio
import os
from pathlib import Path
from typing import Annotated

import typer
import yaml

# Trigger all built-in registrations.
import aidmi_orchestrator.strategy  # noqa: F401
import aidmi_orchestrator.evaluator  # noqa: F401
import aidmi_orchestrator.fixtures  # noqa: F401

from aidmi_orchestrator.benchmark import Benchmark, expand_grid
from aidmi_orchestrator.fixtures.base import get_fixture
from aidmi_orchestrator.strategy.base import make_strategy

app = typer.Typer(add_completion=False, help="aidmi orchestrator runner")


def _require_staging_url() -> str:
    url = os.environ.get("AIDMI_STAGING_DB_URL")
    if not url:
        raise typer.BadParameter(
            "set AIDMI_STAGING_DB_URL to a Postgres connection string."
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
    fixture: Annotated[str, typer.Option(help="registered fixture name")],
    grid: Annotated[Path, typer.Option(help="path to a grid YAML")],
    out: Annotated[Path, typer.Option(help="results directory")],
    runs_per_cell: Annotated[int, typer.Option(help="repetitions per cell")] = 1,
    workspace: Annotated[Path, typer.Option(help="workspace directory")] = Path("./aidmi_workspace"),
):
    """Sweep multiple (strategy, config) cells across a fixture."""
    grid_data = yaml.safe_load(grid.read_text(encoding="utf-8"))
    cells_specs = expand_grid(grid_data)
    cells = [make_strategy(name, cfg) for name, cfg in cells_specs]
    fx = get_fixture(fixture)
    bench = Benchmark(fx, workspace, _require_staging_url())
    out.mkdir(parents=True, exist_ok=True)
    (out / "sweep_config.yaml").write_text(grid.read_text(), encoding="utf-8")
    results = asyncio.run(bench.sweep(cells, runs_per_cell=runs_per_cell, results_path=out / "results.jsonl"))
    typer.echo(f"wrote {len(results)} result rows to {out / 'results.jsonl'}")


if __name__ == "__main__":
    app()

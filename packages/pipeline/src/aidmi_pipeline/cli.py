import tomllib
from pathlib import Path

import typer

from aidmi_pipeline.config import CliMigrationConfig, cli_config_to_run
from aidmi_pipeline.migration import run_migration

app = typer.Typer()


@app.command()
def migrate(config_file: Path = typer.Argument(..., exists=True, readable=True)):
    cfg = CliMigrationConfig.model_validate(tomllib.loads(config_file.read_text()))
    run = cli_config_to_run(cfg)
    result = run_migration(run)

    typer.echo(f"extract: {result.extract.rows_extracted} rows")
    typer.echo(f"transform: {result.transform.overall_status}")
    for m in result.transform.models:
        rows = m.rows_affected if m.rows_affected is not None else "?"
        typer.echo(
            f"  {m.model_name}: {m.status} "
            f"({rows} rows, {m.execution_time_seconds:.1f}s)"
        )
    typer.echo(f"load: {result.load.rows_loaded} rows")

    raise typer.Exit(0 if result.transform.overall_status == "success" else 1)


if __name__ == "__main__":
    app()

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from pydantic import BaseModel


@dataclass
class StagingConfig:
    db_url: str
    dataset_name: str


@dataclass
class MigrationRun:
    source: Any
    staging: StagingConfig
    target: Any
    target_dataset: str
    target_tables: list[str]
    dbt_project_path: Path


class CliMigrationConfig(BaseModel):
    source_kind: str
    source_url: str
    source_table_or_glob: str
    staging_db_url: str
    staging_dataset: str
    target_kind: str
    target_url: str
    target_dataset: str
    target_tables: list[str]
    dbt_project_path: Path


def cli_config_to_run(cfg: CliMigrationConfig) -> MigrationRun:
    import dlt
    from dlt.sources.filesystem import filesystem, read_jsonl
    from dlt.sources.sql_database import sql_database

    if cfg.source_kind == "filesystem":
        source_dir = Path(cfg.source_url)
        source = (
            filesystem(bucket_url=source_dir.as_uri(), file_glob=cfg.source_table_or_glob)
            | read_jsonl()
        ).with_name(Path(cfg.source_table_or_glob).stem)
    elif cfg.source_kind == "sql_database":
        source = sql_database(credentials=cfg.source_url).with_resources(cfg.source_table_or_glob)
    else:
        raise ValueError(f"unknown source_kind: {cfg.source_kind}")

    if cfg.target_kind == "filesystem":
        target_dir = Path(cfg.target_url)
        target = dlt.destinations.filesystem(
            bucket_url=target_dir.as_uri(),
            layout="{table_name}.jsonl",
        )
    elif cfg.target_kind == "postgres":
        target = dlt.destinations.postgres(cfg.target_url)
    else:
        raise ValueError(f"unknown target_kind: {cfg.target_kind}")

    return MigrationRun(
        source=source,
        staging=StagingConfig(db_url=cfg.staging_db_url, dataset_name=cfg.staging_dataset),
        target=target,
        target_dataset=cfg.target_dataset,
        target_tables=cfg.target_tables,
        dbt_project_path=cfg.dbt_project_path,
    )

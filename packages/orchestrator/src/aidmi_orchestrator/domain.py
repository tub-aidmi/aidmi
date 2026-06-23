"""Pydantic types crossing module boundaries.

No I/O, no behavior beyond validation. Keep this module pure.
"""
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict


# ---------- Source side (produced by discover) ----------

class ColumnInfo(BaseModel):
    name: str
    sql_type: str
    nullable: bool


class TableInfo(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    db_schema: str
    name: str
    columns: list[ColumnInfo]
    row_count: int
    sample_rows: list[dict[str, Any]]


class SourceSummary(BaseModel):
    tables: list[TableInfo]


# ---------- Target side ----------

class TargetColumn(BaseModel):
    name: str
    sql_type: str
    nullable: bool = False
    description: str | None = None
    enum_values: list[str] | None = None


class TargetTable(BaseModel):
    name: str
    description: str | None = None
    columns: list[TargetColumn]
    primary_key: list[str] | None = None


class TargetSchema(BaseModel):
    tables: list[TargetTable]


# ---------- LLM config ----------

class ModelSpec(BaseModel):
    provider: str
    model_name: str
    base_url: str | None = None
    api_key_env: str | None = None
    extra: dict[str, Any] = {}


# ---------- Strategy outputs ----------

class ColumnNote(BaseModel):
    target_column: str
    source_columns: list[str] = []
    explanation: str = ""


class TableMappingNote(BaseModel):
    target_table: str
    source_tables: list[str] = []
    column_notes: list[ColumnNote]
    reasoning: str = ""


class MappingManifest(BaseModel):
    tables: list[TableMappingNote]
    strategy_name: str
    strategy_config: dict[str, Any]


class StrategyResult(BaseModel):
    target_tables_written: list[str]
    target_schema: TargetSchema | None = None
    manifest: MappingManifest | None = None
    self_reported_status: Literal["complete", "partial", "gave_up"]


# ---------- Benchmark output ----------

class BenchmarkResult(BaseModel):
    run_id: str
    fixture_name: str
    strategy_name: str
    strategy_spec_name: str
    strategy_config: dict[str, Any]
    rep_index: int = 0
    started_at: datetime
    completed_at: datetime
    wall_clock_seconds: float
    strategy_result: StrategyResult
    metrics: dict[str, Any]
    error: str | None = None
    source_schema: str = ""
    out_schema: str = ""

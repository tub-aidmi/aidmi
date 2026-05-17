from datetime import datetime
from aidmi_orchestrator.domain import (
    ColumnInfo, TableInfo, SourceSummary,
    TargetColumn, TargetTable, TargetSchema,
    ModelSpec,
    ColumnNote, TableMappingNote, MappingManifest,
    StrategyResult, BenchmarkResult,
)


def test_source_summary_round_trip():
    ss = SourceSummary(tables=[
        TableInfo(
            schema="src_xyz",
            name="contacts",
            columns=[ColumnInfo(name="id", sql_type="integer", nullable=False)],
            row_count=8,
            sample_rows=[{"id": 1}],
        ),
    ])
    assert ss.model_dump_json()
    assert SourceSummary.model_validate_json(ss.model_dump_json()) == ss


def test_target_schema_optional_fields():
    t = TargetSchema(tables=[
        TargetTable(name="users", columns=[
            TargetColumn(name="user_id", sql_type="integer"),
            TargetColumn(name="status_enum", sql_type="text", enum_values=["active", "inactive"]),
        ]),
    ])
    assert t.tables[0].columns[1].enum_values == ["active", "inactive"]
    assert t.tables[0].primary_key is None


def test_model_spec_provider_is_free_form_string():
    m = ModelSpec(provider="my_custom_proxy", model_name="x", base_url="http://x", api_key_env="X_KEY")
    assert m.provider == "my_custom_proxy"


def test_strategy_result_minimal():
    r = StrategyResult(
        target_tables_written=["users"],
        self_reported_status="complete",
    )
    assert r.manifest is None
    assert r.target_schema is None


def test_benchmark_result_with_metrics():
    r = BenchmarkResult(
        run_id="01HXX000000000000000000000",
        fixture_name="sp1_users",
        strategy_name="mock",
        strategy_config={"mapping_source": "x.json"},
        started_at=datetime(2026, 5, 17),
        completed_at=datetime(2026, 5, 17),
        wall_clock_seconds=0.5,
        strategy_result=StrategyResult(target_tables_written=["users"], self_reported_status="complete"),
        metrics={"dbt_success": True, "row_count_match": True},
    )
    assert r.metrics["dbt_success"] is True

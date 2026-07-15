from datetime import datetime
from pathlib import Path
import pytest
from aidmi_orchestrator.domain import StrategyResult, ModelSpec
from aidmi_orchestrator.evaluator.base import RunArtifacts, FixtureMetadata
from aidmi_orchestrator.evaluator.execution import ExecutionEvaluator
from aidmi_orchestrator.evaluator.llm_usage import LlmUsageEvaluator
from aidmi_orchestrator.trace import LlmCallEvent


def _artifacts(trace=None, final=None):
    return RunArtifacts(
        run_id="r1",
        dbt_project_path=Path("/tmp/x"),
        dlt_pipelines_dir=Path("/tmp/x/.dlt_pipelines"),
        staging_db_url="postgresql://x",
        source_schema="src_r1_raw",
        out_schema="src_r1_out",
        trace=trace or [],
        strategy_result=StrategyResult(
            target_tables_written=["users"], self_reported_status="complete"
        ),
        target_schema_input=None,
        fixture=FixtureMetadata("f", "desc", None, []),
        wall_clock_seconds=1.0,
        final_transform_result=final,
    )


def test_execution_evaluator_success():
    final = type("TR", (), {
        "overall_status": "success",
        "models": [
            type("M", (), {"status": "success", "model_name": "users", "error_message": None})()
        ],
    })()
    out = ExecutionEvaluator().evaluate(_artifacts(final=final))
    assert out["dbt_success"] is True
    assert out["dbt_models_succeeded"] == 1
    assert out["dbt_models_failed"] == 0


def test_execution_evaluator_no_run():
    out = ExecutionEvaluator().evaluate(_artifacts(final=None))
    assert out["dbt_success"] is False
    assert out["strategy_status"] == "complete"


def test_llm_usage_evaluator_aggregates_by_role():
    # R8 spike: TracedModel records normalized usage keys (cache_read_tokens, not provider-specific).
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
            messages=[], response="ok",
            usage={"input_tokens": 1000, "output_tokens": 500, "cache_read_tokens": 200},
            latency_ms=120.0,
        ),
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
            messages=[], response="ok",
            usage={"input_tokens": 500, "output_tokens": 200, "cache_read_tokens": 0},
            latency_ms=80.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["llm_calls_total"] == 2
    assert out["llm_calls_by_role"] == {"writer": 2}
    assert out["tokens_input_total"] == 1500
    assert out["tokens_input_by_role"] == {"writer": 1500}
    assert out["tokens_input_cached"] == 200
    assert out["tokens_output_total"] == 700
    assert out["tokens_output_by_role"] == {"writer": 700}
    assert 0 <= out["cache_hit_rate"] <= 1
    assert out["latency_ms_total"] >= 0
    assert out["latency_ms_sum_by_role"]["writer"] == 200.0
    assert "writer" in out["latency_ms_p50_by_role"]
    assert out["latency_ms_p50_by_role"]["writer"] >= 0
    assert "writer" in out["latency_ms_p95_by_role"]
    assert out["latency_ms_p95_by_role"]["writer"] >= 0
    assert out["tokens_thoughts_total"] == 0
    assert out["tokens_tool_use_prompt_total"] == 0
    assert out["tokens_input_peak"] == 1000
    assert out["context_utilization_peak"] >= 0
    assert out["usage_details_total"] == {}


def test_llm_usage_evaluator_gemini_details():
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
            messages=[], response="ok",
            usage={
                "input_tokens": 2000,
                "output_tokens": 400,
                "cache_read_tokens": 100,
                "details": {
                    "thoughts_tokens": 50,
                    "tool_use_prompt_tokens": 25,
                    "text_prompt_tokens": 1800,
                },
                "vendor": {"traffic_type": "ON_DEMAND"},
            },
            latency_ms=100.0,
        ),
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="planner",
            model_spec=ModelSpec(provider="google_cloud", model_name="gemini-2.5-flash"),
            messages=[], response="ok",
            usage={
                "input_tokens": 5000,
                "output_tokens": 200,
                "details": {"thoughts_tokens": 10},
            },
            latency_ms=50.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["tokens_thoughts_total"] == 60
    assert out["tokens_thoughts_by_role"] == {"writer": 50, "planner": 10}
    assert out["tokens_tool_use_prompt_total"] == 25
    assert out["tokens_input_peak"] == 5000
    assert out["tokens_input_peak_by_role"] == {"writer": 2000, "planner": 5000}
    assert out["usage_details_total"]["text_prompt_tokens"] == 1800
    assert out["traffic_type_counts"] == {"ON_DEMAND": 1}
    assert out["context_utilization_peak"] > 0
    assert out["context_utilization_peak"] < 0.01
    assert out["dollar_cost_total"] > 0


def test_llm_usage_evaluator_legacy_usage_only():
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
            messages=[], response="ok",
            usage={"input_tokens": 100, "output_tokens": 50, "cache_read_tokens": 0},
            latency_ms=10.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["tokens_thoughts_total"] == 0
    assert out["tokens_tool_use_prompt_total"] == 0
    assert out["tokens_input_peak"] == 100
    assert out["usage_details_total"] == {}


def test_llm_usage_evaluator_malformed_details():
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="openai", model_name="gpt-4o-mini"),
            messages=[], response="ok",
            usage={
                "input_tokens": 100,
                "output_tokens": 50,
                "details": {"thoughts_tokens": "bad", "ok": 3},
            },
            latency_ms=10.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["tokens_thoughts_total"] == 0
    assert out["usage_details_total"] == {"ok": 3}


def test_llm_usage_evaluator_auto_loads_default_pricing(monkeypatch, tmp_path):
    import json
    import aidmi_orchestrator.evaluator.llm_usage as llm_usage_mod

    override = tmp_path / "pricing.json"
    override.write_text(json.dumps({
        "litellm/ise-ollama/qwen3.6:35b-a3b": {
            "input_cost_per_token": 5e-06,
            "output_cost_per_token": 5e-06,
        },
    }))
    monkeypatch.setattr(
        llm_usage_mod, "default_pricing_config_path", lambda: override,
    )
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="litellm", model_name="ise-ollama/qwen3.6:35b-a3b"),
            messages=[], response="ok",
            usage={"input_tokens": 1000, "output_tokens": 500},
            latency_ms=100.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["dollar_cost_total"] == pytest.approx(1500 * 5e-06)


def test_llm_usage_evaluator_unknown_model_no_context_limit():
    trace = [
        LlmCallEvent(
            timestamp=datetime.utcnow(),
            role="writer",
            model_spec=ModelSpec(provider="corporate", model_name="totally-fake-nonexistent-zzz"),
            messages=[], response="ok",
            usage={"input_tokens": 999999, "output_tokens": 1},
            latency_ms=10.0,
        ),
    ]
    out = LlmUsageEvaluator().evaluate(_artifacts(trace=trace))
    assert out["context_utilization_peak"] == 0.0
    assert out["tokens_input_peak"] == 999999


import psycopg2
from aidmi_orchestrator.evaluator.schema import SchemaEvaluator
from aidmi_orchestrator.domain import TargetSchema, TargetTable, TargetColumn


def _materialize(db_url, schema, table, columns: list[tuple[str, str]]):
    cols_sql = ", ".join(f'"{c}" {t}' for c, t in columns)
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            cur.execute(f'CREATE TABLE "{schema}"."{table}" ({cols_sql})')


def test_schema_evaluator_coverage_vs_input(staging_db_url, tmp_path):
    _materialize(staging_db_url, "src_se1_out", "users", [
        ("user_id", "integer"),
        ("firstname", "text"),
        ("email_address", "text"),
        ("extraneous", "text"),
    ])
    target = TargetSchema(tables=[TargetTable(name="users", columns=[
        TargetColumn(name="user_id", sql_type="integer"),
        TargetColumn(name="firstname", sql_type="text"),
        TargetColumn(name="email_address", sql_type="text"),
        TargetColumn(name="status_enum", sql_type="text"),     # missing — uncovered
    ])])
    artifacts = _artifacts()
    artifacts.target_schema_input = target
    artifacts.staging_db_url = staging_db_url
    artifacts.out_schema = "src_se1_out"
    artifacts.strategy_result.target_tables_written = ["users"]
    artifacts.final_transform_result = type("TR", (), {"overall_status": "success", "models": []})()

    out = SchemaEvaluator().evaluate(artifacts)
    assert out["target_columns_covered"] == 3 / 4    # 3 of 4 target columns present
    assert out["extraneous_columns"] == 1
    assert "produced_column_count" in out
    assert out["produced_column_count"] == 4


from aidmi_orchestrator.evaluator.row_equality import (
    RowEqualityEvaluator, ExactComparator, FuzzyComparator,
)


def test_exact_comparator_identical():
    cmp = ExactComparator()
    assert cmp.compare_row({"a": 1, "b": "x"}, {"a": 1, "b": "x"}) is True
    assert cmp.compare_row({"a": 1, "b": "x"}, {"a": 1, "b": "y"}) is False


def test_fuzzy_comparator_whitespace():
    cmp = FuzzyComparator(normalize_whitespace=True)
    assert cmp.compare_row({"a": "hello  world"}, {"a": "hello world"}) is True


def test_fuzzy_comparator_case_insensitive():
    cmp = FuzzyComparator(case_insensitive=True)
    assert cmp.compare_row({"a": "ALICE"}, {"a": "alice"}) is True

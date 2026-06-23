from pathlib import Path

from aidmi_orchestrator.fixtures.base import Fixture, register_fixture

HERE = Path(__file__).parent

register_fixture(
    Fixture(
        name="mock",
        description="Minimal contacts → users fixture for deterministic mock strategy smoke tests.",
        source_schema="fixture_mock_src",
        source_sql_path=HERE / "source.sql",
        destination_sql_path=HERE / "destination.sql",
        target_schema_path=HERE / "target_schema.json",
        reference_dbt_path=HERE / "reference_dbt",
        applicable_evaluators=["execution", "llm_usage", "schema", "row_equality"],
    )
)

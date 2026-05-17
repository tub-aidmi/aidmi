"""sp1_users fixture: single-table contacts → users (SP1's reference)."""
from pathlib import Path
from aidmi_orchestrator.fixtures.base import register_fixture, Fixture

HERE = Path(__file__).parent


def _load_source():
    from dlt.sources.filesystem import filesystem, read_jsonl
    return (
        filesystem(bucket_url=f"file://{HERE / 'source'}", file_glob="*.jsonl")
        | read_jsonl()
    ).with_name("contacts")


register_fixture(Fixture(
    name="sp1_users",
    description="Single-table contacts → users transformation; carries forward SP1's reference.",
    source_factory=_load_source,
    target_schema_path=HERE / "target_schema.json",
    reference_dbt_path=HERE / "reference_dbt",
    applicable_evaluators=["execution", "llm_usage", "schema", "row_equality"],
))

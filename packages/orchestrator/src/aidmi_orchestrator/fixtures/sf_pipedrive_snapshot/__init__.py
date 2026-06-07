"""Hermetic JSONL snapshot of the Salesforce Contact+Account slice (sandbox data)."""
from pathlib import Path

from aidmi_orchestrator.fixtures.base import Fixture, register_fixture

HERE = Path(__file__).parent
_LIVE = HERE.parent / "sf_pipedrive"


def _load_source():
    import dlt
    from dlt.sources.filesystem import filesystem, read_jsonl

    @dlt.source(name="salesforce_snapshot")
    def snapshot_source():
        contact = (
            filesystem(bucket_url=f"file://{HERE / 'source'}", file_glob="contact.jsonl")
            | read_jsonl()
        ).with_name("contact")
        account = (
            filesystem(bucket_url=f"file://{HERE / 'source'}", file_glob="account.jsonl")
            | read_jsonl()
        ).with_name("account")
        return contact, account

    return snapshot_source()


register_fixture(Fixture(
    name="sf_pipedrive_snapshot",
    description="Committed JSONL snapshot of the Salesforce Contact+Account slice; hermetic benchmark variant of sf_pipedrive.",
    source_factory=_load_source,
    target_schema_path=_LIVE / "target_schema.json",
    reference_dbt_path=None,
    applicable_evaluators=["execution", "llm_usage", "schema", "manifest_quality", "data_preservation"],
))

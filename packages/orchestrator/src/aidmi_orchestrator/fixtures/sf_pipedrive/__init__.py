from pathlib import Path
import os

from aidmi_orchestrator.fixtures.base import Fixture, register_fixture
from aidmi_orchestrator.fixtures.sf_pipedrive.salesforce.fixture_source import (
    salesforce_fixture_slice,
)

HERE = Path(__file__).parent


def _load_source():
    u = os.environ.get("SF_USERNAME") or ""
    p = os.environ.get("SF_PASSWORD") or ""
    t = os.environ.get("SF_SECURITY_TOKEN") or ""
    if not u.strip() or not p.strip() or not t.strip():
        raise RuntimeError(
            "Missing Salesforce credentials in environment; set SF_USERNAME, SF_PASSWORD, "
            "and SF_SECURITY_TOKEN (non-empty). Login uses login.salesforce.com "
            "(no env override)."
        )
    return salesforce_fixture_slice().with_resources("contact", "account")


register_fixture(
    Fixture(
        name="sf_pipedrive",
        description="Salesforce Contact + Account into staging; LLM maps to Pipedrive-shaped persons and organizations.",
        source_factory=_load_source,
        target_schema_path=HERE / "target_schema.json",
        reference_dbt_path=None,
        applicable_evaluators=["execution", "llm_usage", "schema"],
    )
)

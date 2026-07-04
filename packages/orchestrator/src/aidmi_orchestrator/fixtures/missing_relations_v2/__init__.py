from aidmi_orchestrator.fixtures._register_sql import DEFAULT_EVALUATORS_V2, register_sql_fixture

register_sql_fixture(
    "missing_relations_v2",
    "v2: Broken/ambiguous relationships → Salesforce target; destination includes ground truth.",
    golden_schema="fixture_missing_relations_v2_golden",
    evaluators=DEFAULT_EVALUATORS_V2,
)

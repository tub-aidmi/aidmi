from aidmi_orchestrator.fixtures._register_sql import DEFAULT_EVALUATORS_V2, register_sql_fixture

register_sql_fixture(
    "messy_data_v2",
    "v2: Messy field values + duplicates → Salesforce target; destination includes ground truth.",
    golden_schema="fixture_messy_data_v2_golden",
    evaluators=DEFAULT_EVALUATORS_V2,
)

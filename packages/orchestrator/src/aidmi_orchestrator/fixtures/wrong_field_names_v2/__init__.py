from aidmi_orchestrator.fixtures._register_sql import register_sql_fixture

register_sql_fixture(
    "wrong_field_names_v2",
    "v2: German table/column names → Salesforce target; destination includes ground truth.",
    golden_schema="fixture_wrong_field_names_v2_golden",
)

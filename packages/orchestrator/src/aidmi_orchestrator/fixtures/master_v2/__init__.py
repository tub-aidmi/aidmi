from aidmi_orchestrator.fixtures._register_sql import register_sql_fixture

register_sql_fixture(
    "master_v2",
    "v2: German master_* tables + messy data + broken relationships; destination includes ground truth.",
    golden_schema="fixture_master_v2_golden",
)

from aidmi_orchestrator.fixtures._register_sql import register_sql_fixture

register_sql_fixture(
    "missing_relations",
    "Implicit/denormalized relations without FK constraints → Salesforce target.",
)

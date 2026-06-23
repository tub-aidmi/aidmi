from aidmi_orchestrator.fixtures._register_sql import register_sql_fixture

register_sql_fixture(
    "master",
    "German master_* tables → Salesforce Account/Contact/Opportunity/Project/Asset target.",
)

from pathlib import Path

import pytest

from aidmi_orchestrator.ddl_target_schema import parse_create_table, parse_ddl_file
from aidmi_orchestrator.domain import TargetSchema

FIXTURES = Path(__file__).resolve().parents[2] / "src" / "aidmi_orchestrator" / "fixtures"


def test_parse_account_table():
    ddl = (FIXTURES / "master" / "destination.sql").read_text(encoding="utf-8")
    account_ddl = ddl.splitlines()[3]
    table = parse_create_table(account_ddl)

    assert table.name == "Account"
    assert table.primary_key == ["Id"]
    assert len(table.columns) == 13

    tier = next(c for c in table.columns if c.name == "Customer_Tier__c")
    assert tier.enum_values == ["Gold", "Silver", "Bronze", "Platinum"]
    assert tier.nullable is True

    name_col = next(c for c in table.columns if c.name == "Name")
    assert name_col.nullable is False


def test_parse_opportunity_stage_enum():
    ddl = (FIXTURES / "master" / "destination.sql").read_text(encoding="utf-8")
    opp_ddl = next(line for line in ddl.splitlines() if "Opportunity" in line and line.startswith("CREATE"))
    table = parse_create_table(opp_ddl)

    stage = next(c for c in table.columns if c.name == "StageName")
    assert stage.nullable is False
    assert "Closed Won" in stage.enum_values
    assert len(stage.enum_values) == 10


def test_parse_full_destination_file():
    schema = parse_ddl_file((FIXTURES / "master" / "destination.sql").read_text(encoding="utf-8"))
    assert isinstance(schema, TargetSchema)
    assert {t.name for t in schema.tables} == {
        "Account",
        "Contact",
        "Opportunity",
        "Project__c",
        "Installed_Asset__c",
    }


def test_generated_json_roundtrip():
    from aidmi_orchestrator.scripts.gen_target_schema import generate

    out = FIXTURES / "master" / "target_schema.json"
    generate(FIXTURES / "master" / "destination.sql", out)
    schema = TargetSchema.model_validate_json(out.read_text(encoding="utf-8"))
    assert schema.tables[0].name == "Account"

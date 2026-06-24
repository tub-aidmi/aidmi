"""Pure-function tests for the CRM case importer."""
import sqlite3
from pathlib import Path

from scripts.import_crm_cases import (
    extract_ground_truth,
    target_schema_from_db,
    NEUTRAL_DESCRIPTIONS,
)


def _make_dest(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE Account (
            Id TEXT, Name TEXT NOT NULL, Legacy_Customer_ID__c TEXT, IsDeleted INTEGER
        );
        CREATE TABLE Opportunity (Id TEXT, Amount REAL);
        CREATE TABLE _field_mapping (
            id INTEGER PRIMARY KEY, source_table TEXT, source_column TEXT,
            target_table TEXT, target_column TEXT, transform TEXT, notes TEXT
        );
        CREATE TABLE _migration_log (id INTEGER PRIMARY KEY);
        INSERT INTO _field_mapping (source_table, source_column, target_table, target_column, transform, notes)
        VALUES ('kunden','nr','Account','Legacy_Customer_ID__c',NULL,'External ID'),
               ('kunden','name','Account','Name',NULL,NULL);
        """
    )
    con.commit()
    con.close()


def test_extract_ground_truth_reads_field_mapping(tmp_path):
    dest = tmp_path / "dest.db"
    _make_dest(dest)
    gt = extract_ground_truth(dest, case="demo")
    assert gt["case"] == "demo"
    assert len(gt["edges"]) == 2
    first = gt["edges"][0]
    assert first == {
        "source_table": "kunden", "source_column": "nr",
        "target_table": "Account", "target_column": "Legacy_Customer_ID__c",
        "transform": None, "notes": "External ID",
    }


def test_target_schema_skips_meta_tables_and_maps_types(tmp_path):
    dest = tmp_path / "dest.db"
    _make_dest(dest)
    schema = target_schema_from_db(dest)
    table_names = {t["name"] for t in schema["tables"]}
    assert table_names == {"Account", "Opportunity"}  # _field_mapping / _migration_log excluded
    account = next(t for t in schema["tables"] if t["name"] == "Account")
    cols = {c["name"]: c for c in account["columns"]}
    assert cols["Name"]["sql_type"] == "text"
    assert cols["Name"]["nullable"] is False           # NOT NULL
    assert cols["Legacy_Customer_ID__c"]["nullable"] is True
    assert cols["IsDeleted"]["sql_type"] == "integer"
    opp = next(t for t in schema["tables"] if t["name"] == "Opportunity")
    assert {c["name"]: c["sql_type"] for c in opp["columns"]}["Amount"] == "double precision"
    # neutral descriptions are applied where known, never sourced from _field_mapping
    assert cols["Name"]["description"] == NEUTRAL_DESCRIPTIONS[("Account", "Name")]

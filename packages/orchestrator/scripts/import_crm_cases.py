"""One-shot importer: convert the CRM source/destination SQLite pairs into
committed fixture artifacts (source.db copies, ground_truth.json, shared
target_schema.json).

Usage:
    python -m scripts.import_crm_cases --input-dir ~/Downloads

Idempotent: rewrites the generated files in the crm_salesforce fixture package.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from pathlib import Path

# case key -> (source filename, destination filename)
CASES = {
    "master": ("master_source.db", "master_destination.db"),
    "wrong_field_names": ("wrong_field_names_source.db", "wrong_field_names_destination.db"),
    "messy_data": ("messy_data_source.db", "messy_data_destination.db"),
    "missing_relations": ("missing_relations_source.db", "missing_relations_destination.db"),
}

META_TABLES = {"_field_mapping", "_migration_log", "sqlite_sequence"}

# Neutral descriptions for the shared Salesforce target. Describe the FIELD only —
# never the source mapping or transform (no answer leak into LLM context).
NEUTRAL_DESCRIPTIONS = {
    ("Account", "Id"): "Target record identifier (assigned on load).",
    ("Account", "Name"): "Account / company name.",
    ("Account", "ERP_Number__c"): "ERP system number for the account.",
    ("Account", "Customer_Tier__c"): "Customer tier or classification.",
    ("Account", "Region__c"): "Sales region.",
    ("Account", "Industry"): "Industry classification.",
    ("Account", "Website"): "Company website URL.",
    ("Account", "BillingCity"): "Billing address city.",
    ("Account", "BillingCountry"): "Billing address country.",
    ("Account", "Legacy_Customer_ID__c"): "External identifier carried from the source system.",
    ("Account", "CreatedDate"): "Record creation timestamp.",
    ("Account", "LastModifiedDate"): "Record last-modified timestamp.",
    ("Account", "IsDeleted"): "Soft-delete flag.",
    ("Contact", "Id"): "Target record identifier (assigned on load).",
    ("Contact", "FirstName"): "Contact first name.",
    ("Contact", "LastName"): "Contact last name.",
    ("Contact", "Email"): "Primary email address.",
    ("Contact", "Phone"): "Phone number.",
    ("Contact", "Title"): "Job title.",
    ("Contact", "Role__c"): "Contact role.",
    ("Contact", "Preferred_Language__c"): "Preferred correspondence language.",
    ("Contact", "AccountId"): "Reference to the related Account.",
    ("Contact", "Legacy_Contact_ID__c"): "External identifier carried from the source system.",
    ("Contact", "CreatedDate"): "Record creation timestamp.",
    ("Contact", "LastModifiedDate"): "Record last-modified timestamp.",
    ("Contact", "IsDeleted"): "Soft-delete flag.",
    ("Installed_Asset__c", "Id"): "Target record identifier (assigned on load).",
    ("Installed_Asset__c", "Name"): "Asset name.",
    ("Installed_Asset__c", "Serial_Number__c"): "Asset serial number.",
    ("Installed_Asset__c", "Warranty_End_Date__c"): "Warranty end date.",
    ("Installed_Asset__c", "Account__c"): "Reference to the related Account.",
    ("Installed_Asset__c", "Project__c"): "Reference to the related Project.",
    ("Installed_Asset__c", "Legacy_Asset_ID__c"): "External identifier carried from the source system.",
    ("Installed_Asset__c", "CreatedDate"): "Record creation timestamp.",
    ("Installed_Asset__c", "LastModifiedDate"): "Record last-modified timestamp.",
    ("Installed_Asset__c", "IsDeleted"): "Soft-delete flag.",
    ("Opportunity", "Id"): "Target record identifier (assigned on load).",
    ("Opportunity", "Name"): "Opportunity name.",
    ("Opportunity", "StageName"): "Sales stage.",
    ("Opportunity", "CloseDate"): "Expected or actual close date.",
    ("Opportunity", "Amount"): "Monetary amount.",
    ("Opportunity", "CurrencyIsoCode"): "ISO currency code.",
    ("Opportunity", "AccountId"): "Reference to the related Account.",
    ("Opportunity", "Legacy_Opportunity_ID__c"): "External identifier carried from the source system.",
    ("Opportunity", "CreatedDate"): "Record creation timestamp.",
    ("Opportunity", "LastModifiedDate"): "Record last-modified timestamp.",
    ("Opportunity", "IsDeleted"): "Soft-delete flag.",
    ("Project__c", "Id"): "Target record identifier (assigned on load).",
    ("Project__c", "Name"): "Project name.",
    ("Project__c", "Project_Status__c"): "Project status.",
    ("Project__c", "Go_Live_Date__c"): "Go-live date.",
    ("Project__c", "Account__c"): "Reference to the related Account.",
    ("Project__c", "Opportunity__c"): "Reference to the related Opportunity.",
    ("Project__c", "Legacy_Project_ID__c"): "External identifier carried from the source system.",
    ("Project__c", "CreatedDate"): "Record creation timestamp.",
    ("Project__c", "LastModifiedDate"): "Record last-modified timestamp.",
    ("Project__c", "IsDeleted"): "Soft-delete flag.",
}

# Default primary key for every target table.
_PK = ["Id"]


def _sqlite_type_to_pg(sqlite_type: str) -> str:
    t = (sqlite_type or "").strip().upper()
    if t.startswith("INT"):
        return "integer"
    if t in {"REAL", "DOUBLE", "FLOAT", "NUMERIC"} or t.startswith("DOUBLE") or t.startswith("NUM"):
        return "double precision"
    return "text"


def extract_ground_truth(dest_db: Path, case: str) -> dict:
    """Read _field_mapping into the ground_truth.json structure."""
    con = sqlite3.connect(dest_db)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT source_table, source_column, target_table, target_column, transform, notes "
        "FROM _field_mapping ORDER BY id"
    ).fetchall()
    con.close()
    edges = [
        {
            "source_table": r["source_table"],
            "source_column": r["source_column"],
            "target_table": r["target_table"],
            "target_column": r["target_column"],
            "transform": r["transform"],
            "notes": r["notes"],
        }
        for r in rows
    ]
    return {"case": case, "edges": edges}


def target_schema_from_db(dest_db: Path) -> dict:
    """Build the TargetSchema dict from the destination DB schema, with neutral
    descriptions. Meta tables (_field_mapping, _migration_log, sqlite_sequence) excluded."""
    con = sqlite3.connect(dest_db)
    con.row_factory = sqlite3.Row
    table_names = [
        r["name"]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        if r["name"] not in META_TABLES
    ]
    tables = []
    for tname in table_names:
        cols = []
        for c in con.execute(f'PRAGMA table_info("{tname}")'):
            cols.append({
                "name": c["name"],
                "sql_type": _sqlite_type_to_pg(c["type"]),
                "nullable": c["notnull"] == 0,
                "description": NEUTRAL_DESCRIPTIONS.get((tname, c["name"])),
            })
        tables.append({"name": tname, "description": None, "columns": cols, "primary_key": _PK})
    con.close()
    return {"tables": tables}


def _fixture_root() -> Path:
    return Path(__file__).resolve().parent.parent / "src" / "aidmi_orchestrator" / "fixtures" / "crm_salesforce"


def main() -> None:
    parser = argparse.ArgumentParser(description="Import CRM SQLite cases into fixture artifacts.")
    parser.add_argument("--input-dir", default="~/Downloads", help="dir containing the *_source.db / *_destination.db pairs")
    args = parser.parse_args()
    input_dir = Path(args.input_dir).expanduser()
    root = _fixture_root()

    schemas: list[str] = []
    for case, (src_name, dest_name) in CASES.items():
        src = input_dir / src_name
        dest = input_dir / dest_name
        if not src.exists() or not dest.exists():
            raise SystemExit(f"missing input for case {case}: {src} / {dest}")

        gt = extract_ground_truth(dest, case)
        case_dir = root / "cases" / case
        case_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, case_dir / "source.db")
        (case_dir / "ground_truth.json").write_text(
            json.dumps(gt, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        schemas.append(json.dumps(target_schema_from_db(dest), sort_keys=True))
        print(f"imported {case}: {len(gt['edges'])} edges")

    # All four destinations share an identical target schema; assert and write once.
    if len(set(schemas)) != 1:
        raise SystemExit(f"destination target schemas differ across cases: {len(set(schemas))} distinct")
    (root / "target_schema.json").write_text(
        json.dumps(target_schema_from_db(input_dir / CASES["master"][1]), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote shared target_schema.json")


if __name__ == "__main__":
    main()

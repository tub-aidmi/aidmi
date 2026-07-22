"""Golden target-schema SQL and the ground-truth mapping rows."""

from __future__ import annotations

from typing import Any

from aidmi_orchestrator.scripts.fixtures_gen.dataset import survivor
from aidmi_orchestrator.scripts.fixtures_gen.sql import format_inserts, schema_header

DEST_DDL_PG = """
CREATE TABLE "Account" ( "Id" text PRIMARY KEY, "Name" text NOT NULL, "ERP_Number__c" text UNIQUE, "Customer_Tier__c" text CHECK ("Customer_Tier__c" IN ('Gold','Silver','Bronze','Platinum')), "Region__c" text, "Industry" text, "Website" text, "BillingCity" text, "BillingCountry" text, "Legacy_Customer_ID__c" text UNIQUE, "CreatedDate" text DEFAULT CURRENT_TIMESTAMP, "LastModifiedDate" text DEFAULT CURRENT_TIMESTAMP, "IsDeleted" integer DEFAULT 0 );

CREATE TABLE "Contact" ( "Id" text PRIMARY KEY, "FirstName" text, "LastName" text NOT NULL, "Email" text, "Phone" text, "Title" text, "Role__c" text CHECK ("Role__c" IN ('Decision Maker','End User','Technical Contact','Executive Sponsor')), "Preferred_Language__c" text CHECK ("Preferred_Language__c" IN ('DE','EN','FR','ES','IT')), "AccountId" text REFERENCES "Account"("Id"), "Legacy_Contact_ID__c" text UNIQUE, "CreatedDate" text DEFAULT CURRENT_TIMESTAMP, "LastModifiedDate" text DEFAULT CURRENT_TIMESTAMP, "IsDeleted" integer DEFAULT 0 );

CREATE TABLE "Opportunity" ( "Id" text PRIMARY KEY, "Name" text NOT NULL, "StageName" text NOT NULL CHECK ("StageName" IN ( 'Prospecting','Qualification','Needs Analysis', 'Value Proposition','Id. Decision Makers', 'Perception Analysis','Proposal/Price Quote', 'Negotiation/Review','Closed Won','Closed Lost')), "CloseDate" text NOT NULL, "Amount" double precision, "CurrencyIsoCode" text DEFAULT 'EUR', "AccountId" text REFERENCES "Account"("Id"), "Legacy_Opportunity_ID__c" text UNIQUE, "CreatedDate" text DEFAULT CURRENT_TIMESTAMP, "LastModifiedDate" text DEFAULT CURRENT_TIMESTAMP, "IsDeleted" integer DEFAULT 0 );

CREATE TABLE "Project__c" ( "Id" text PRIMARY KEY, "Name" text NOT NULL, "Project_Status__c" text CHECK ("Project_Status__c" IN ('Active','Completed','In Planning','On Hold','Cancelled')), "Go_Live_Date__c" text, "Account__c" text REFERENCES "Account"("Id"), "Opportunity__c" text REFERENCES "Opportunity"("Id"), "Legacy_Project_ID__c" text UNIQUE, "CreatedDate" text DEFAULT CURRENT_TIMESTAMP, "LastModifiedDate" text DEFAULT CURRENT_TIMESTAMP, "IsDeleted" integer DEFAULT 0 );

CREATE TABLE "Installed_Asset__c" ( "Id" text PRIMARY KEY, "Name" text NOT NULL, "Serial_Number__c" text UNIQUE, "Warranty_End_Date__c" text, "Account__c" text REFERENCES "Account"("Id"), "Project__c" text REFERENCES "Project__c"("Id"), "Legacy_Asset_ID__c" text UNIQUE, "CreatedDate" text DEFAULT CURRENT_TIMESTAMP, "LastModifiedDate" text DEFAULT CURRENT_TIMESTAMP, "IsDeleted" integer DEFAULT 0 );
""".strip()

GROUND_TRUTH_DDL_PG = """
CREATE TABLE _ground_truth (
    id SERIAL PRIMARY KEY,
    target_table text,
    target_id text,
    source_table text,
    source_id text,
    notes text
);
""".strip()


def _acc_of(rec: dict[str, Any]) -> str | None:
    p = rec["parent"]
    return survivor(p)["did"] if p else None


def write_destination_pg(
    data: dict[str, list[dict[str, Any]]], golden_schema: str
) -> str:
    gt: list[tuple[str, str, str, str, str | None]] = []

    def truth(tbl: str, tid: str, src_tbl: str, src_id: str, notes: str | None) -> None:
        gt.append((tbl, tid, src_tbl, src_id, notes))

    account_rows: list[tuple[Any, ...]] = []
    for a in data["accounts"]:
        if a["is_dup"]:
            truth(
                "Account",
                survivor(a)["legacy"],
                "source_account",
                a["legacy"],
                a["notes"],
            )
            continue
        account_rows.append(
            (
                a["did"],
                a["name"],
                a["erp"],
                a["tier"],
                a["region"] or None,
                a["industry"],
                a["website"],
                a["city"] or None,
                a["country"],
                a["legacy"],
            )
        )
        truth("Account", a["did"], "source_account", a["legacy"], a["notes"])

    contact_rows: list[tuple[Any, ...]] = []
    for c in data["contacts"]:
        contact_rows.append(
            (
                c["did"],
                c["first"],
                c["last"],
                c["email"],
                c["phone"],
                c["title"],
                c["role"],
                c["lang"],
                _acc_of(c),
                c["legacy"],
            )
        )
        truth("Contact", c["did"], "source_contact", c["legacy"], c["notes"])

    opp_rows: list[tuple[Any, ...]] = []
    for o in data["opps"]:
        opp_rows.append(
            (
                o["did"],
                o["name"],
                o["stage"],
                o["close"],
                o["amount"],
                o["cur"],
                _acc_of(o),
                o["legacy"],
            )
        )
        truth("Opportunity", o["did"], "source_opportunity", o["legacy"], o["notes"])

    proj_rows: list[tuple[Any, ...]] = []
    for p in data["projs"]:
        opp_id = p["opp"]["did"] if p["opp"] else None
        proj_rows.append(
            (
                p["did"],
                p["name"],
                p["status"],
                p["golive"],
                _acc_of(p),
                opp_id,
                p["legacy"],
            )
        )
        truth("Project__c", p["did"], "source_project", p["legacy"], p["notes"])

    asset_rows: list[tuple[Any, ...]] = []
    for s in data["assets"]:
        proj_id = s["proj"]["did"] if s["proj"] else None
        asset_rows.append(
            (
                s["did"],
                s["name"],
                s["serial"],
                s["warranty"],
                _acc_of(s),
                proj_id,
                s["legacy"],
            )
        )
        truth("Installed_Asset__c", s["did"], "source_asset", s["legacy"], s["notes"])

    parts = [
        schema_header(golden_schema),
        "-- Target schema + ground truth data",
        DEST_DDL_PG,
        "",
        format_inserts(
            '"Account"',
            [
                "Id",
                "Name",
                "ERP_Number__c",
                "Customer_Tier__c",
                "Region__c",
                "Industry",
                "Website",
                "BillingCity",
                "BillingCountry",
                "Legacy_Customer_ID__c",
            ],
            account_rows,
            quote_columns=True,
        ),
        format_inserts(
            '"Contact"',
            [
                "Id",
                "FirstName",
                "LastName",
                "Email",
                "Phone",
                "Title",
                "Role__c",
                "Preferred_Language__c",
                "AccountId",
                "Legacy_Contact_ID__c",
            ],
            contact_rows,
            quote_columns=True,
        ),
        format_inserts(
            '"Opportunity"',
            [
                "Id",
                "Name",
                "StageName",
                "CloseDate",
                "Amount",
                "CurrencyIsoCode",
                "AccountId",
                "Legacy_Opportunity_ID__c",
            ],
            opp_rows,
            quote_columns=True,
        ),
        format_inserts(
            '"Project__c"',
            [
                "Id",
                "Name",
                "Project_Status__c",
                "Go_Live_Date__c",
                "Account__c",
                "Opportunity__c",
                "Legacy_Project_ID__c",
            ],
            proj_rows,
            quote_columns=True,
        ),
        format_inserts(
            '"Installed_Asset__c"',
            [
                "Id",
                "Name",
                "Serial_Number__c",
                "Warranty_End_Date__c",
                "Account__c",
                "Project__c",
                "Legacy_Asset_ID__c",
            ],
            asset_rows,
            quote_columns=True,
        ),
        GROUND_TRUTH_DDL_PG,
        format_inserts(
            "_ground_truth",
            ["target_table", "target_id", "source_table", "source_id", "notes"],
            gt,
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"

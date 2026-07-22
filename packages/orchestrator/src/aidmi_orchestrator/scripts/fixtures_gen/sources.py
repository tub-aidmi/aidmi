"""Per-problem source-system SQL writers."""

from __future__ import annotations

import random
from typing import Any

from aidmi_orchestrator.scripts.fixtures_gen.sql import format_inserts, schema_header
from aidmi_orchestrator.scripts.fixtures_gen.variants import typo


def _acc_ref(rec: dict[str, Any], by_name: bool = False) -> str:
    p = rec["parent"]
    if p is None:
        return "Unknown" if by_name else f"MISSING-{random.randint(9000, 9999):04d}"
    return p["name"] if by_name else p["legacy"]


def _account_fields(a: dict[str, Any]) -> tuple[Any, ...]:
    """The nine source-side account columns every problem emits, in order."""
    return (
        a["legacy"],
        a["name_src"],
        a["erp"],
        a["tier_src"],
        a["region"],
        a["industry_src"],
        a["website"],
        a["city"],
        a["country"],
    )


def _contact_head(c: dict[str, Any]) -> tuple[Any, ...]:
    return (
        c["legacy"],
        c["first"],
        c["last"],
        c["email_src"],
        c["phone_src"],
        c["title"],
        c["role_src"],
        c["lang_src"],
    )


def _opp_head(o: dict[str, Any]) -> tuple[Any, ...]:
    return (o["legacy"], o["name"], o["stage_src"], o["close_src"])


def _proj_head(p: dict[str, Any]) -> tuple[Any, ...]:
    return (p["legacy"], p["name"], p["status_src"], p["golive_src"])


def _asset_head(s: dict[str, Any]) -> tuple[Any, ...]:
    return (s["legacy"], s["name"], s["serial"], s["warranty_src"])


def write_source_p1_pg(
    data: dict[str, list[dict[str, Any]]], source_schema: str
) -> str:
    ddl = """
CREATE TABLE kunden (kunden_nr text PRIMARY KEY, firmenname text, erp_nummer text,
    kategorie text, gebiet text, branche text, webseite text, ort text, land text);
CREATE TABLE ansprechpartner (ap_id text PRIMARY KEY, vorname text, nachname text,
    email_adresse text, telefonnummer text, position text, funktion text, sprache text, kunde text);
CREATE TABLE chancen (chance_id text PRIMARY KEY, bezeichnung text, phase text,
    abschlussdatum text, volumen double precision, waehrung text, kd_nr text);
CREATE TABLE proj (proj_id text PRIMARY KEY, name text, status text, go_live text, kd text, opp text);
CREATE TABLE assets (asset_id text PRIMARY KEY, bezeichnung text, seriennr text,
    garantie_bis text, kd_ref text, projekt_ref text);
""".strip()
    parts = [
        schema_header(source_schema),
        ddl,
        format_inserts(
            "kunden",
            [
                "kunden_nr",
                "firmenname",
                "erp_nummer",
                "kategorie",
                "gebiet",
                "branche",
                "webseite",
                "ort",
                "land",
            ],
            [_account_fields(a) for a in data["accounts"]],
        ),
        format_inserts(
            "ansprechpartner",
            [
                "ap_id",
                "vorname",
                "nachname",
                "email_adresse",
                "telefonnummer",
                "position",
                "funktion",
                "sprache",
                "kunde",
            ],
            [_contact_head(c) + (_acc_ref(c),) for c in data["contacts"]],
        ),
        format_inserts(
            "chancen",
            [
                "chance_id",
                "bezeichnung",
                "phase",
                "abschlussdatum",
                "volumen",
                "waehrung",
                "kd_nr",
            ],
            [
                _opp_head(o) + (o["amount_src"], o["cur_src"], _acc_ref(o))
                for o in data["opps"]
            ],
        ),
        format_inserts(
            "proj",
            ["proj_id", "name", "status", "go_live", "kd", "opp"],
            [
                _proj_head(p) + (_acc_ref(p), p["opp"]["legacy"] if p["opp"] else None)
                for p in data["projs"]
            ],
        ),
        format_inserts(
            "assets",
            [
                "asset_id",
                "bezeichnung",
                "seriennr",
                "garantie_bis",
                "kd_ref",
                "projekt_ref",
            ],
            [
                _asset_head(s)
                + (_acc_ref(s), s["proj"]["legacy"] if s["proj"] else None)
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def write_source_p2_pg(
    data: dict[str, list[dict[str, Any]]], source_schema: str
) -> str:
    ddl = """
CREATE TABLE Account (Id text PRIMARY KEY, Name text, ERP_Number__c text,
    Customer_Tier__c text, Region__c text, Industry text, Website text,
    BillingCity text, BillingCountry text, Legacy_Customer_ID__c text);
CREATE TABLE Contact (Id text PRIMARY KEY, FirstName text, LastName text, Email text,
    Phone text, Title text, Role__c text, Preferred_Language__c text, AccountId text);
CREATE TABLE Opportunity (Id text PRIMARY KEY, Name text, StageName text,
    CloseDate text, Amount text, CurrencyIsoCode text, AccountId text);
CREATE TABLE Project__c (Id text PRIMARY KEY, Name text, Project_Status__c text,
    Go_Live_Date__c text, Account__c text, Opportunity__c text);
CREATE TABLE Installed_Asset__c (Id text PRIMARY KEY, Name text, Serial_Number__c text,
    Warranty_End_Date__c text, Account__c text, Project__c text);
""".strip()
    parts = [
        schema_header(source_schema),
        ddl,
        format_inserts(
            "Account",
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
            [_account_fields(a) + (a["legacy"],) for a in data["accounts"]],
        ),
        format_inserts(
            "Contact",
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
            ],
            [_contact_head(c) + (c["parent"]["legacy"],) for c in data["contacts"]],
        ),
        format_inserts(
            "Opportunity",
            [
                "Id",
                "Name",
                "StageName",
                "CloseDate",
                "Amount",
                "CurrencyIsoCode",
                "AccountId",
            ],
            [
                _opp_head(o)
                + (str(o["amount_src"]), o["cur_src"], o["parent"]["legacy"])
                for o in data["opps"]
            ],
        ),
        format_inserts(
            "Project__c",
            [
                "Id",
                "Name",
                "Project_Status__c",
                "Go_Live_Date__c",
                "Account__c",
                "Opportunity__c",
            ],
            [
                _proj_head(p)
                + (
                    p["parent"]["legacy"],
                    p["opp"]["legacy"] if p["opp"] else None,
                )
                for p in data["projs"]
            ],
        ),
        format_inserts(
            "Installed_Asset__c",
            [
                "Id",
                "Name",
                "Serial_Number__c",
                "Warranty_End_Date__c",
                "Account__c",
                "Project__c",
            ],
            [
                _asset_head(s)
                + (
                    s["parent"]["legacy"],
                    s["proj"]["legacy"] if s["proj"] else None,
                )
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def write_source_p3_pg(
    data: dict[str, list[dict[str, Any]]], source_schema: str
) -> str:
    ddl = """
CREATE TABLE Account (id text PRIMARY KEY, name text, tier text, region text, industry text);
CREATE TABLE Contact (id text PRIMARY KEY, full_name text, email text,
    account_ref text, company_name text);
CREATE TABLE Opportunity (id text PRIMARY KEY, name text, stage text, amount double precision,
    customer_number text, account_name text);
CREATE TABLE Project (id text PRIMARY KEY, name text, status text, go_live text,
    client_id text, opportunity_ref text);
CREATE TABLE Asset (id text PRIMARY KEY, name text, serial text, warranty text,
    client text, project text);
""".strip()

    def stale(name: str) -> str:
        return typo(name) if random.random() < 0.08 else name

    contact_rows: list[tuple[Any, ...]] = []
    for c in data["contacts"]:
        p = c["parent"]
        if p is None:
            ref, company = f"ACC-{random.randint(9000, 9999):04d}", "Unknown"
        elif random.random() < 0.12:
            ref, company = None, stale(p["name"])
        else:
            ref, company = p["legacy"], stale(p["name"])
        contact_rows.append(
            (c["legacy"], f"{c['first']} {c['last']}", c["email"], ref, company)
        )

    asset_rows: list[tuple[Any, ...]] = []
    for s in data["assets"]:
        p = s["parent"]
        if p is None:
            client = f"ACC-{random.randint(9000, 9999):04d}"
        else:
            client = p["name"] if random.random() < 0.5 else p["legacy"]
        proj_ref = (
            s["proj"]["legacy"]
            if s["proj"]
            else f"PROJ-{random.randint(9000, 9999):04d}"
        )
        asset_rows.append(
            (s["legacy"], s["name"], s["serial"], s["warranty"], client, proj_ref)
        )

    parts = [
        schema_header(source_schema),
        ddl,
        format_inserts(
            "Account",
            ["id", "name", "tier", "region", "industry"],
            [
                (a["legacy"], a["name"], a["tier"], a["region"], a["industry"])
                for a in data["accounts"]
            ],
        ),
        format_inserts(
            "Contact",
            ["id", "full_name", "email", "account_ref", "company_name"],
            contact_rows,
        ),
        format_inserts(
            "Opportunity",
            ["id", "name", "stage", "amount", "customer_number", "account_name"],
            [
                (
                    o["legacy"],
                    o["name"],
                    o["stage"],
                    o["amount"],
                    (
                        o["parent"]["legacy"]
                        .replace("CUST-", "KD-")
                        .replace("ACC-", "KD-")
                        if o["parent"]
                        else f"KD-{random.randint(9000, 9999):04d}"
                    ),
                    stale(o["parent"]["name"]) if o["parent"] else "Unknown",
                )
                for o in data["opps"]
            ],
        ),
        format_inserts(
            "Project",
            ["id", "name", "status", "go_live", "client_id", "opportunity_ref"],
            [
                (
                    p["legacy"],
                    p["name"],
                    p["status"],
                    p["golive"],
                    p["parent"]["legacy"]
                    if p["parent"]
                    else f"ACC-{random.randint(9000, 9999):04d}",
                    p["opp"]["legacy"]
                    if p["opp"]
                    else f"OPP-{random.randint(9000, 9999):04d}",
                )
                for p in data["projs"]
            ],
        ),
        format_inserts(
            "Asset",
            ["id", "name", "serial", "warranty", "client", "project"],
            asset_rows,
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def write_source_p4_pg(
    data: dict[str, list[dict[str, Any]]], source_schema: str
) -> str:
    ddl = """
CREATE TABLE master_kunden (kundennummer text PRIMARY KEY, unternehmensname text, erp_nr text,
    kundenklasse text, vertriebsgebiet text, industrie text, homepage text, stadt text, land_region text);
CREATE TABLE master_kontakte (kontakt_id text PRIMARY KEY, rufname text, familienname text,
    kontakt_email text, tel text, berufsbezeichnung text, rolle text, korrespondenzsprache text, kd_nummer text);
CREATE TABLE master_opportunities (opp_kennung text PRIMARY KEY, titel text, vertriebsphase text,
    zieldatum text, auftragswert text, waehrungscode text, kunden_ref text);
CREATE TABLE master_projekte (projekt_kennung text PRIMARY KEY, projektname text, projektstatus text,
    go_live_datum text, kunden_kennung text, opp_kennung_ref text);
CREATE TABLE master_assets (asset_kennung text PRIMARY KEY, asset_name text, serien_nummer text,
    garantieende text, kunden_kennung text, projekt_kennung text);
""".strip()
    parts = [
        schema_header(source_schema),
        ddl,
        format_inserts(
            "master_kunden",
            [
                "kundennummer",
                "unternehmensname",
                "erp_nr",
                "kundenklasse",
                "vertriebsgebiet",
                "industrie",
                "homepage",
                "stadt",
                "land_region",
            ],
            [_account_fields(a) for a in data["accounts"]],
        ),
        format_inserts(
            "master_kontakte",
            [
                "kontakt_id",
                "rufname",
                "familienname",
                "kontakt_email",
                "tel",
                "berufsbezeichnung",
                "rolle",
                "korrespondenzsprache",
                "kd_nummer",
            ],
            [_contact_head(c) + (_acc_ref(c),) for c in data["contacts"]],
        ),
        format_inserts(
            "master_opportunities",
            [
                "opp_kennung",
                "titel",
                "vertriebsphase",
                "zieldatum",
                "auftragswert",
                "waehrungscode",
                "kunden_ref",
            ],
            [
                _opp_head(o)
                + (
                    str(o["amount_src"]),
                    o["cur_src"],
                    (
                        o["parent"]["legacy"].replace("CUST-", "KD-")
                        if o["parent"]
                        else f"KD-M{random.randint(9000, 9999):04d}"
                    ),
                )
                for o in data["opps"]
            ],
        ),
        format_inserts(
            "master_projekte",
            [
                "projekt_kennung",
                "projektname",
                "projektstatus",
                "go_live_datum",
                "kunden_kennung",
                "opp_kennung_ref",
            ],
            [
                _proj_head(p)
                + (
                    _acc_ref(p),
                    p["opp"]["legacy"]
                    if p["opp"]
                    else f"OPP-M-{random.randint(99000, 99999):05d}",
                )
                for p in data["projs"]
            ],
        ),
        format_inserts(
            "master_assets",
            [
                "asset_kennung",
                "asset_name",
                "serien_nummer",
                "garantieende",
                "kunden_kennung",
                "projekt_kennung",
            ],
            [
                _asset_head(s)
                + (
                    _acc_ref(s),
                    s["proj"]["legacy"]
                    if s["proj"]
                    else f"PROJ-M-{random.randint(99000, 99999):05d}",
                )
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"

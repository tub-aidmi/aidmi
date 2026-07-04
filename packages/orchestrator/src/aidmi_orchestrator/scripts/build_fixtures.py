"""Generate v2 Postgres SQL fixtures from canonical-first synthetic CRM data."""
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any, Callable

from faker import Faker

from aidmi_orchestrator.ddl_target_schema import parse_ddl_file

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
SEED = 42
fake = Faker("de_DE")

# ─────────────────────────────────────────────────────────────────────────────
# CANONICAL ↔ VARIANT TABLES
# ─────────────────────────────────────────────────────────────────────────────

STAGE_VARIANTS = {
    "Prospecting": ["Prospecting", "prospecting", "PROSPECTING", "Prospect", "In Kontakt"],
    "Qualification": ["Qualification", "Qualifikation", "qualification", "Quali", "In Prüfung"],
    "Closed Won": ["Closed Won", "closed won", "Gewonnen", "Won", "Abgeschlossen (Gewonnen)"],
    "Closed Lost": ["Closed Lost", "Verloren", "lost", "Abgeschlossen (Verloren)", "LOST"],
}
TIER_VARIANTS = {
    "Gold": ["Gold", "gold", "GOLD"],
    "Silver": ["Silver", "silver", "SILBER"],
    "Bronze": ["Bronze", "bronze", "BRONZE"],
    "Platinum": ["Platinum", "Platin", "platinum"],
}
INDUSTRY_VARIANTS = {
    "Technology": ["Technology", "Technologie", "IT"],
    "Finance": ["Finance", "Finanzen"],
    "Healthcare": ["Healthcare", "Gesundheitswesen"],
    "Manufacturing": ["Manufacturing", "Industrie"],
}
STATUS_VARIANTS = {
    "Active": ["Active", "active", "Aktiv"],
    "Completed": ["Completed", "completed", "Abgeschlossen"],
    "In Planning": ["In Planning", "In Planung", "Planung"],
    "On Hold": ["On Hold", "Pausiert", "on hold"],
    "Cancelled": ["Cancelled", "Storniert", "cancelled"],
}
ROLE_VARIANTS = {
    "Decision Maker": ["Decision Maker", "Entscheider", "decision maker"],
    "End User": ["End User", "Endanwender", "end user"],
    "Technical Contact": ["Technical Contact", "Technischer Ansprechpartner", "Techniker"],
    "Executive Sponsor": ["Executive Sponsor", "Sponsor"],
}
LANG_VARIANTS = {
    "DE": ["DE", "de", "Deutsch", "deutsch", "German"],
    "EN": ["EN", "en", "English", "englisch", "Englisch"],
    "FR": ["FR", "fr", "Französisch", "French"],
}
CURRENCY_VARIANTS = {
    "EUR": ["EUR", "eur", "€", "Euro"],
    "USD": ["USD", "Dollar", "usd", "$"],
    "GBP": ["GBP", "gbp", "£"],
    "CHF": ["CHF", "chf"],
}

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

PROBLEM_TO_FIXTURE = {
    "p1": "wrong_field_names_v2",
    "p2": "messy_data_v2",
    "p3": "missing_relations_v2",
    "p4": "master_v2",
}

_id_counters: dict[str, int] = {}


def reset(seed_offset: int) -> None:
    random.seed(SEED + seed_offset)
    Faker.seed(SEED + seed_offset)
    _id_counters.clear()


def dest_id(prefix: str) -> str:
    _id_counters[prefix] = _id_counters.get(prefix, 0) + 1
    return f"{prefix}{_id_counters[prefix]:012d}"


def variant(canon: str, table: dict[str, list[str]]) -> str:
    return random.choice(table[canon])


def uniq(gen: Callable[[], str], seen: set[str]) -> str:
    while True:
        v = gen()
        if v not in seen:
            seen.add(v)
            return v


def survivor(acc: dict[str, Any]) -> dict[str, Any]:
    while acc.get("dup_of"):
        acc = acc["dup_of"]
    return acc


def typo(s: str) -> str:
    s = str(s)
    if len(s) < 3:
        return s
    i = random.randint(1, len(s) - 2)
    return s[:i] + s[i + 1] + s[i] + s[i + 2 :]


def sql_val(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def format_inserts(table: str, columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        return ""
    col_list = ", ".join(f'"{c.strip(chr(34))}"' for c in columns)
    value_lines = []
    for row in rows:
        vals = ", ".join(sql_val(v) for v in row)
        value_lines.append(f"  ({vals})")
    return f"INSERT INTO {table} ({col_list}) VALUES\n" + ",\n".join(value_lines) + ";"


def schema_header(schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema};\nSET search_path TO {schema};\n"


def gen_phone(messy: bool) -> tuple[str | None, Any]:
    digits = "".join(str(random.randint(0, 9)) for _ in range(10))
    canonical = f"+49{digits}"
    if not messy:
        return canonical, canonical
    if random.random() < 0.18:
        return None, random.choice(["N/A", "", None])
    src = random.choice([
        digits,
        f"+49{digits}",
        f"+49 {digits[:4]} {digits[4:]}",
        f"({digits[:4]}) {digits[4:]}",
    ])
    return canonical, src


def gen_date(
    messy: bool,
    start: str = "-3y",
    end: str = "+2y",
    allow_missing: bool = True,
) -> tuple[str | None, Any]:
    d = fake.date_between(start_date=start, end_date=end)
    iso = d.isoformat()
    if not messy:
        return iso, iso
    if allow_missing and random.random() < 0.15:
        return None, random.choice(["0000-00-00", "N/A", None])
    src = random.choice([
        iso,
        f"{d.day:02d}.{d.month:02d}.{d.year}",
        f"{d.month}/{d.day}/{d.year}",
        f"{d.year}{d.month:02d}{d.day:02d}",
    ])
    return iso, src


def gen_amount(messy: bool) -> tuple[float | None, Any]:
    base = round(random.uniform(1000, 500000), 2)
    if not messy:
        return base, base
    if random.random() < 0.10:
        return None, random.choice([None, 0])
    de = f"{base:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    src = random.choice([base, de, f"EUR {base}", -abs(base)])
    return base, src


def gen_email(messy: bool) -> tuple[str | None, Any]:
    good = fake.email()
    if not messy:
        return good, good
    if random.random() < 0.08:
        return None, random.choice([None, "", "N/A", "invalid@mail"])
    return good, good


def gen_dataset(
    n_accounts: int,
    *,
    messy: bool,
    orphans: dict[str, float],
    dup_count: int,
    seed_offset: int,
    legacy_prefix: str,
) -> dict[str, list[dict[str, Any]]]:
    reset(seed_offset)
    rate = lambda k: orphans.get(k, 0.0)

    accounts: list[dict[str, Any]] = []
    contacts: list[dict[str, Any]] = []
    opps: list[dict[str, Any]] = []
    projs: list[dict[str, Any]] = []
    assets: list[dict[str, Any]] = []
    erp_seen: set[str] = set()
    serial_seen: set[str] = set()

    def note(x: list[str | None]) -> str | None:
        return ";".join(n for n in x if n) or None

    for i in range(1, n_accounts + 1):
        legacy = f"{legacy_prefix}{1000 + i}"
        tier = random.choice(list(TIER_VARIANTS))
        ind = random.choice(list(INDUSTRY_VARIANTS))
        name = fake.company()
        notes: list[str] = []
        name_src = name
        if messy and random.random() < 0.05:
            name_src, name = None, name
            notes.append("name_missing_in_source")
        elif messy and random.random() < 0.04:
            name_src = typo(name)
            notes.append("name_typo_in_source")
        erp = uniq(lambda: f"ERP-{random.randint(10000, 99999)}", erp_seen)
        accounts.append({
            "did": dest_id("001"),
            "legacy": legacy,
            "dup_of": None,
            "name": name,
            "name_src": name_src,
            "erp": erp,
            "tier": tier,
            "tier_src": variant(tier, TIER_VARIANTS) if messy else tier,
            "region": random.choice(["DACH", "Nordics", "Benelux", "UK", "Southern Europe"]),
            "industry": ind,
            "industry_src": variant(ind, INDUSTRY_VARIANTS) if messy else ind,
            "website": fake.url(),
            "city": fake.city(),
            "country": fake.country(),
            "is_dup": False,
            "notes": note(notes),
        })

    for d in random.sample(accounts, min(dup_count, len(accounts))):
        dup = dict(d)
        dup["did"] = dest_id("001")
        dup["legacy"] = d["legacy"] + "_DUP"
        dup["is_dup"] = True
        dup["dup_of"] = d
        dup["notes"] = note([d["notes"], "duplicate_account"])
        accounts.append(dup)

    def parented(acc: dict[str, Any], key: str) -> tuple[dict[str, Any] | None, bool]:
        if random.random() < rate(key):
            return None, True
        return acc, False

    def rand_parent(
        pool: list[dict[str, Any]], key: str
    ) -> tuple[dict[str, Any] | None, bool]:
        if not pool or random.random() < rate(key):
            return None, True
        return random.choice(pool), False

    for acc in accounts:
        for _ in range(random.randint(1, 3)):
            parent, orphan = parented(acc, "c_acc")
            role = random.choice(list(ROLE_VARIANTS))
            lang = random.choice(list(LANG_VARIANTS))
            email_clean, email_src = gen_email(messy)
            phone_clean, phone_src = gen_phone(messy)
            role_clean, role_src = role, (variant(role, ROLE_VARIANTS) if messy else role)
            notes = []
            if messy and random.random() < 0.10:
                role_clean, role_src = None, random.choice(["", "N/A", None])
                notes.append("role_unmapped")
            if email_clean is None:
                notes.append("email_invalid_removed")
            if phone_clean is None:
                notes.append("phone_unrecoverable")
            if orphan:
                notes.append("orphan_nulled")
            contacts.append({
                "did": dest_id("003"),
                "legacy": f"CON-{len(contacts) + 1:05d}",
                "first": fake.first_name(),
                "last": fake.last_name(),
                "email": email_clean,
                "email_src": email_src,
                "phone": phone_clean,
                "phone_src": phone_src,
                "title": fake.job(),
                "role": role_clean,
                "role_src": role_src,
                "lang": lang,
                "lang_src": variant(lang, LANG_VARIANTS) if messy else lang,
                "parent": parent,
                "orphan": orphan,
                "notes": note(notes),
            })

    for acc in accounts:
        for _ in range(random.randint(1, 4)):
            parent, orphan = parented(acc, "o_acc")
            stage = random.choice(list(STAGE_VARIANTS))
            cur = random.choice(list(CURRENCY_VARIANTS))
            date_clean, date_src = gen_date(messy, "-2y", "+1y", allow_missing=False)
            amt_clean, amt_src = gen_amount(messy)
            notes = []
            if amt_clean is None:
                notes.append("amount_missing")
            if orphan:
                notes.append("orphan_nulled")
            opps.append({
                "did": dest_id("006"),
                "legacy": f"OPP-{len(opps) + 1:05d}",
                "name": fake.bs().title(),
                "stage": stage,
                "stage_src": variant(stage, STAGE_VARIANTS) if messy else stage,
                "close": date_clean,
                "close_src": date_src,
                "amount": amt_clean,
                "amount_src": amt_src,
                "cur": cur,
                "cur_src": variant(cur, CURRENCY_VARIANTS) if messy else cur,
                "parent": parent,
                "orphan": orphan,
                "notes": note(notes),
            })

    for acc in accounts:
        for _ in range(random.randint(0, 3)):
            parent, orphan = parented(acc, "p_acc")
            opp_parent, opp_orphan = rand_parent(opps, "p_opp")
            status = random.choice(list(STATUS_VARIANTS))
            gl_clean, gl_src = gen_date(messy, "-1y", "+2y", allow_missing=True)
            notes = []
            if orphan:
                notes.append("account_orphan_nulled")
            if opp_orphan:
                notes.append("opp_orphan_nulled")
            projs.append({
                "did": dest_id("a00"),
                "legacy": f"PROJ-{len(projs) + 1:05d}",
                "name": f"{fake.company()} Impl.",
                "status": status,
                "status_src": variant(status, STATUS_VARIANTS) if messy else status,
                "golive": gl_clean,
                "golive_src": gl_src,
                "parent": parent,
                "orphan": orphan,
                "opp": opp_parent,
                "opp_orphan": opp_orphan,
                "notes": note(notes),
            })

    n_assets = max(4 * n_accounts, 1)
    for _ in range(n_assets):
        parent, orphan = rand_parent(accounts, "a_acc")
        proj_parent, proj_orphan = rand_parent(projs, "a_proj")
        wr_clean, wr_src = gen_date(messy, "+1y", "+5y", allow_missing=True)
        notes = []
        if orphan:
            notes.append("account_orphan_nulled")
        if proj_orphan:
            notes.append("project_orphan_nulled")
        assets.append({
            "did": dest_id("a01"),
            "legacy": f"AST-{len(assets) + 1:05d}",
            "name": random.choice([
                "CRM Seat",
                "API Bundle",
                "Support Package",
                "Data Connector",
                "Mobile App",
                "Analytics Add-on",
            ]),
            "serial": uniq(lambda: f"SN-{random.randint(10000000, 99999999)}", serial_seen),
            "warranty": wr_clean,
            "warranty_src": wr_src,
            "parent": parent,
            "orphan": orphan,
            "proj": proj_parent,
            "proj_orphan": proj_orphan,
            "notes": note(notes),
        })

    return {
        "accounts": accounts,
        "contacts": contacts,
        "opps": opps,
        "projs": projs,
        "assets": assets,
    }


def _acc_of(rec: dict[str, Any]) -> str | None:
    p = rec["parent"]
    return survivor(p)["did"] if p else None


def write_destination_pg(data: dict[str, list[dict[str, Any]]], golden_schema: str) -> str:
    gt: list[tuple[str, str, str, str, str | None]] = []

    def truth(
        tbl: str, tid: str, src_tbl: str, src_id: str, notes: str | None
    ) -> None:
        gt.append((tbl, tid, src_tbl, src_id, notes))

    account_rows: list[tuple[Any, ...]] = []
    for a in data["accounts"]:
        if a["is_dup"]:
            truth("Account", survivor(a)["did"], "source_account", a["legacy"], a["notes"])
            continue
        account_rows.append((
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
        ))
        truth("Account", a["did"], "source_account", a["legacy"], a["notes"])

    contact_rows: list[tuple[Any, ...]] = []
    for c in data["contacts"]:
        contact_rows.append((
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
        ))
        truth("Contact", c["did"], "source_contact", c["legacy"], c["notes"])

    opp_rows: list[tuple[Any, ...]] = []
    for o in data["opps"]:
        opp_rows.append((
            o["did"],
            o["name"],
            o["stage"],
            o["close"],
            o["amount"],
            o["cur"],
            _acc_of(o),
            o["legacy"],
        ))
        truth("Opportunity", o["did"], "source_opportunity", o["legacy"], o["notes"])

    proj_rows: list[tuple[Any, ...]] = []
    for p in data["projs"]:
        opp_id = p["opp"]["did"] if p["opp"] else None
        proj_rows.append((
            p["did"],
            p["name"],
            p["status"],
            p["golive"],
            _acc_of(p),
            opp_id,
            p["legacy"],
        ))
        truth("Project__c", p["did"], "source_project", p["legacy"], p["notes"])

    asset_rows: list[tuple[Any, ...]] = []
    for s in data["assets"]:
        proj_id = s["proj"]["did"] if s["proj"] else None
        asset_rows.append((
            s["did"],
            s["name"],
            s["serial"],
            s["warranty"],
            _acc_of(s),
            proj_id,
            s["legacy"],
        ))
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
        ),
        GROUND_TRUTH_DDL_PG,
        format_inserts(
            "_ground_truth",
            ["target_table", "target_id", "source_table", "source_id", "notes"],
            gt,
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def _acc_ref(rec: dict[str, Any], by_name: bool = False) -> str:
    p = rec["parent"]
    if p is None:
        return "Unknown" if by_name else f"MISSING-{random.randint(9000, 9999):04d}"
    return p["name"] if by_name else p["legacy"]


def write_source_p1_pg(data: dict[str, list[dict[str, Any]]], source_schema: str) -> str:
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
            [
                (
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
                for a in data["accounts"]
            ],
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
            [
                (
                    c["legacy"],
                    c["first"],
                    c["last"],
                    c["email_src"],
                    c["phone_src"],
                    c["title"],
                    c["role_src"],
                    c["lang_src"],
                    _acc_ref(c),
                )
                for c in data["contacts"]
            ],
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
                (
                    o["legacy"],
                    o["name"],
                    o["stage_src"],
                    o["close_src"],
                    o["amount_src"],
                    o["cur_src"],
                    _acc_ref(o),
                )
                for o in data["opps"]
            ],
        ),
        format_inserts(
            "proj",
            ["proj_id", "name", "status", "go_live", "kd", "opp"],
            [
                (
                    p["legacy"],
                    p["name"],
                    p["status_src"],
                    p["golive_src"],
                    _acc_ref(p),
                    p["opp"]["legacy"] if p["opp"] else None,
                )
                for p in data["projs"]
            ],
        ),
        format_inserts(
            "assets",
            ["asset_id", "bezeichnung", "seriennr", "garantie_bis", "kd_ref", "projekt_ref"],
            [
                (
                    s["legacy"],
                    s["name"],
                    s["serial"],
                    s["warranty_src"],
                    _acc_ref(s),
                    s["proj"]["legacy"] if s["proj"] else None,
                )
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def write_source_p2_pg(data: dict[str, list[dict[str, Any]]], source_schema: str) -> str:
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
            [
                (
                    a["legacy"],
                    a["name_src"],
                    a["erp"],
                    a["tier_src"],
                    a["region"],
                    a["industry_src"],
                    a["website"],
                    a["city"],
                    a["country"],
                    a["legacy"],
                )
                for a in data["accounts"]
            ],
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
            [
                (
                    c["legacy"],
                    c["first"],
                    c["last"],
                    c["email_src"],
                    c["phone_src"],
                    c["title"],
                    c["role_src"],
                    c["lang_src"],
                    c["parent"]["legacy"],
                )
                for c in data["contacts"]
            ],
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
                (
                    o["legacy"],
                    o["name"],
                    o["stage_src"],
                    o["close_src"],
                    str(o["amount_src"]),
                    o["cur_src"],
                    o["parent"]["legacy"],
                )
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
                (
                    p["legacy"],
                    p["name"],
                    p["status_src"],
                    p["golive_src"],
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
                (
                    s["legacy"],
                    s["name"],
                    s["serial"],
                    s["warranty_src"],
                    s["parent"]["legacy"],
                    s["proj"]["legacy"] if s["proj"] else None,
                )
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


def write_source_p3_pg(data: dict[str, list[dict[str, Any]]], source_schema: str) -> str:
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
        contact_rows.append((c["legacy"], f"{c['first']} {c['last']}", c["email"], ref, company))

    asset_rows: list[tuple[Any, ...]] = []
    for s in data["assets"]:
        p = s["parent"]
        if p is None:
            client = f"ACC-{random.randint(9000, 9999):04d}"
        else:
            client = p["name"] if random.random() < 0.5 else p["legacy"]
        proj_ref = s["proj"]["legacy"] if s["proj"] else f"PROJ-{random.randint(9000, 9999):04d}"
        asset_rows.append((s["legacy"], s["name"], s["serial"], s["warranty"], client, proj_ref))

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
                        o["parent"]["legacy"].replace("CUST-", "KD-").replace("ACC-", "KD-")
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
                    p["parent"]["legacy"] if p["parent"] else f"ACC-{random.randint(9000, 9999):04d}",
                    p["opp"]["legacy"] if p["opp"] else f"OPP-{random.randint(9000, 9999):04d}",
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


def write_source_p4_pg(data: dict[str, list[dict[str, Any]]], source_schema: str) -> str:
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
            [
                (
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
                for a in data["accounts"]
            ],
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
            [
                (
                    c["legacy"],
                    c["first"],
                    c["last"],
                    c["email_src"],
                    c["phone_src"],
                    c["title"],
                    c["role_src"],
                    c["lang_src"],
                    _acc_ref(c),
                )
                for c in data["contacts"]
            ],
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
                (
                    o["legacy"],
                    o["name"],
                    o["stage_src"],
                    o["close_src"],
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
                (
                    p["legacy"],
                    p["name"],
                    p["status_src"],
                    p["golive_src"],
                    _acc_ref(p),
                    p["opp"]["legacy"] if p["opp"] else f"OPP-M-{random.randint(99000, 99999):05d}",
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
                (
                    s["legacy"],
                    s["name"],
                    s["serial"],
                    s["warranty_src"],
                    _acc_ref(s),
                    s["proj"]["legacy"] if s["proj"] else f"PROJ-M-{random.randint(99000, 99999):05d}",
                )
                for s in data["assets"]
            ],
        ),
    ]
    return "\n\n".join(p for p in parts if p) + "\n"


PROBLEMS: dict[str, dict[str, Any]] = {
    "p1": {
        "n_accounts": 100,
        "messy": False,
        "orphans": {},
        "dup_count": 0,
        "legacy_prefix": "CUST-",
        "seed_offset": 1,
        "writer": write_source_p1_pg,
    },
    "p2": {
        "n_accounts": 120,
        "messy": True,
        "orphans": {},
        "dup_count": 6,
        "legacy_prefix": "CUST-",
        "seed_offset": 2,
        "writer": write_source_p2_pg,
    },
    "p3": {
        "n_accounts": 80,
        "messy": False,
        "orphans": {"c_acc": 0.15, "p_opp": 0.20, "a_proj": 0.15},
        "dup_count": 0,
        "legacy_prefix": "ACC-",
        "seed_offset": 3,
        "writer": write_source_p3_pg,
    },
    "p4": {
        "n_accounts": 200,
        "messy": True,
        "orphans": {
            "c_acc": 0.12,
            "o_acc": 0.10,
            "p_acc": 0.10,
            "a_acc": 0.10,
            "p_opp": 0.20,
            "a_proj": 0.15,
        },
        "dup_count": 10,
        "legacy_prefix": "CUST-M",
        "seed_offset": 4,
        "writer": write_source_p4_pg,
    },
}


def build_fixture(pkey: str) -> dict[str, int]:
    cfg = PROBLEMS[pkey]
    fixture_name = PROBLEM_TO_FIXTURE[pkey]
    source_schema = f"fixture_{fixture_name}_src"
    golden_schema = f"fixture_{fixture_name}_golden"

    data = gen_dataset(
        cfg["n_accounts"],
        messy=cfg["messy"],
        orphans=cfg["orphans"],
        dup_count=cfg["dup_count"],
        seed_offset=cfg["seed_offset"],
        legacy_prefix=cfg["legacy_prefix"],
    )

    out_dir = FIXTURES_DIR / fixture_name
    out_dir.mkdir(parents=True, exist_ok=True)

    source_sql = cfg["writer"](data, source_schema)
    destination_sql = write_destination_pg(data, golden_schema)

    (out_dir / "source.sql").write_text(source_sql, encoding="utf-8")
    (out_dir / "destination.sql").write_text(destination_sql, encoding="utf-8")

    schema = parse_ddl_file(DEST_DDL_PG)
    (out_dir / "target_schema.json").write_text(
        json.dumps(schema.model_dump(exclude_none=True), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    counts = {k: len(v) for k, v in data.items()}
    print(f"  {fixture_name}: {counts}")
    return counts


def main() -> None:
    labels = {
        "p1": "Wrong Field & Object Names",
        "p2": "Messy Data",
        "p3": "Missing / Unclear Relationships",
        "p4": "Master Dataset (all problems)",
    }
    for pkey in ["p1", "p2", "p3", "p4"]:
        print(f"Building {pkey.upper()} — {labels[pkey]} ...")
        build_fixture(pkey)
    print("Done.")


if __name__ == "__main__":
    main()

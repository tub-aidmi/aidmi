"""Canonical synthetic CRM dataset generation and its seeded RNG state."""

from __future__ import annotations

import random
from collections.abc import Callable
from typing import Any

from faker import Faker

from aidmi_orchestrator.scripts.fixtures_gen import SEED, fake
from aidmi_orchestrator.scripts.fixtures_gen.messiness import (
    gen_amount,
    gen_date,
    gen_email,
    gen_phone,
)
from aidmi_orchestrator.scripts.fixtures_gen.variants import (
    CURRENCY_VARIANTS,
    INDUSTRY_VARIANTS,
    LANG_VARIANTS,
    ROLE_VARIANTS,
    STAGE_VARIANTS,
    STATUS_VARIANTS,
    TIER_VARIANTS,
    typo,
    variant,
)

_id_counters: dict[str, int] = {}


def reset(seed_offset: int) -> None:
    random.seed(SEED + seed_offset)
    Faker.seed(SEED + seed_offset)
    _id_counters.clear()


def dest_id(prefix: str) -> str:
    _id_counters[prefix] = _id_counters.get(prefix, 0) + 1
    return f"{prefix}{_id_counters[prefix]:012d}"


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

    def rate(k):
        return orphans.get(k, 0.0)

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
        accounts.append(
            {
                "did": dest_id("001"),
                "legacy": legacy,
                "dup_of": None,
                "name": name,
                "name_src": name_src,
                "erp": erp,
                "tier": tier,
                "tier_src": variant(tier, TIER_VARIANTS) if messy else tier,
                "region": random.choice(
                    ["DACH", "Nordics", "Benelux", "UK", "Southern Europe"]
                ),
                "industry": ind,
                "industry_src": variant(ind, INDUSTRY_VARIANTS) if messy else ind,
                "website": fake.url(),
                "city": fake.city(),
                "country": fake.country(),
                "is_dup": False,
                "notes": note(notes),
            }
        )

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
            role_clean, role_src = (
                role,
                (variant(role, ROLE_VARIANTS) if messy else role),
            )
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
            contacts.append(
                {
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
                }
            )

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
            opps.append(
                {
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
                }
            )

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
            projs.append(
                {
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
                }
            )

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
        assets.append(
            {
                "did": dest_id("a01"),
                "legacy": f"AST-{len(assets) + 1:05d}",
                "name": random.choice(
                    [
                        "CRM Seat",
                        "API Bundle",
                        "Support Package",
                        "Data Connector",
                        "Mobile App",
                        "Analytics Add-on",
                    ]
                ),
                "serial": uniq(
                    lambda: f"SN-{random.randint(10000000, 99999999)}", serial_seen
                ),
                "warranty": wr_clean,
                "warranty_src": wr_src,
                "parent": parent,
                "orphan": orphan,
                "proj": proj_parent,
                "proj_orphan": proj_orphan,
                "notes": note(notes),
            }
        )

    return {
        "accounts": accounts,
        "contacts": contacts,
        "opps": opps,
        "projs": projs,
        "assets": assets,
    }

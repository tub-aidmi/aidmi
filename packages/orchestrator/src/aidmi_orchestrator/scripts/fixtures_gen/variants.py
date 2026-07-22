"""Canonical value tables and the messiness helpers that pick from them."""

from __future__ import annotations

import random

STAGE_VARIANTS = {
    "Prospecting": [
        "Prospecting",
        "prospecting",
        "PROSPECTING",
        "Prospect",
        "In Kontakt",
    ],
    "Qualification": [
        "Qualification",
        "Qualifikation",
        "qualification",
        "Quali",
        "In Prüfung",
    ],
    "Closed Won": [
        "Closed Won",
        "closed won",
        "Gewonnen",
        "Won",
        "Abgeschlossen (Gewonnen)",
    ],
    "Closed Lost": [
        "Closed Lost",
        "Verloren",
        "lost",
        "Abgeschlossen (Verloren)",
        "LOST",
    ],
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
    "Technical Contact": [
        "Technical Contact",
        "Technischer Ansprechpartner",
        "Techniker",
    ],
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


def variant(canon: str, table: dict[str, list[str]]) -> str:
    return random.choice(table[canon])


def typo(s: str) -> str:
    s = str(s)
    if len(s) < 3:
        return s
    i = random.randint(1, len(s) - 2)
    return s[:i] + s[i + 1] + s[i] + s[i + 2 :]

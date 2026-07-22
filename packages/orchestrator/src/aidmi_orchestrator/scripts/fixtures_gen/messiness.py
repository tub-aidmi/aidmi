"""Field-level generators that produce a canonical value and a messy source value."""

from __future__ import annotations

import random
from typing import Any

from aidmi_orchestrator.scripts.fixtures_gen import fake


def gen_phone(messy: bool) -> tuple[str | None, Any]:
    digits = "".join(str(random.randint(0, 9)) for _ in range(10))
    canonical = f"+49{digits}"
    if not messy:
        return canonical, canonical
    if random.random() < 0.18:
        return None, random.choice(["N/A", "", None])
    src = random.choice(
        [
            digits,
            f"+49{digits}",
            f"+49 {digits[:4]} {digits[4:]}",
            f"({digits[:4]}) {digits[4:]}",
        ]
    )
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
    src = random.choice(
        [
            iso,
            f"{d.day:02d}.{d.month:02d}.{d.year}",
            f"{d.month}/{d.day}/{d.year}",
            f"{d.year}{d.month:02d}{d.day:02d}",
        ]
    )
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

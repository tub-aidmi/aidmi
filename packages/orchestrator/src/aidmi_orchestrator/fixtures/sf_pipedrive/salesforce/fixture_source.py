"""Minimal Salesforce source: Contact + Account only (aidmi SF → Pipedrive mapping fixture)."""

from __future__ import annotations

import os
from typing import Iterable, Any

import dlt
from dlt.common.typing import TDataItem
from simple_salesforce import Salesforce

from .helpers.records import get_records


def _strip_cred(key: str) -> str | None:
    v = os.environ.get(key)
    if v is None:
        return None
    stripped = v.strip()
    return stripped if stripped else None


def _salesforce_client() -> Salesforce:
    username = _strip_cred("SF_USERNAME")
    password = _strip_cred("SF_PASSWORD")
    security_token = _strip_cred("SF_SECURITY_TOKEN")
    if not username or not password or not security_token:
        raise RuntimeError(
            "SF_USERNAME, SF_PASSWORD, and SF_SECURITY_TOKEN must each be "
            "non-empty after trimming whitespace."
        )
    return Salesforce(
        username=username,
        password=password,
        security_token=security_token,
    )


@dlt.source(name="salesforce")
def salesforce_fixture_slice() -> Iterable[Any]:
    sf = _salesforce_client()

    @dlt.resource(write_disposition="replace")
    def contact() -> Iterable[TDataItem]:
        yield from get_records(sf, "Contact")

    @dlt.resource(write_disposition="replace")
    def account() -> Iterable[TDataItem]:
        yield from get_records(sf, "Account")

    return (contact, account)

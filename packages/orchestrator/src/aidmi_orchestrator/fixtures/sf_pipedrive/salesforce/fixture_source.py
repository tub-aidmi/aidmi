"""Minimal Salesforce source: Contact + Account only (aidmi SF → Pipedrive mapping fixture)."""

from __future__ import annotations

import os
from typing import Any, Iterable

import dlt
from dlt.common.typing import TDataItem
from simple_salesforce import Salesforce

from .helpers.records import get_records


def _salesforce_client() -> Salesforce:
    domain = os.environ.get("SF_DOMAIN", "login")
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
        domain=domain,
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

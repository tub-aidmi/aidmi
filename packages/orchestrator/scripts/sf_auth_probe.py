"""Verify Salesforce SOAP login from `.env`."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from aidmi_orchestrator.fixtures.sf_pipedrive.salesforce.fixture_source import (
    _salesforce_client,
)

load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=True)


def main() -> None:
    try:
        sf = _salesforce_client()
    except RuntimeError as e:
        sys.exit(str(e))
    except SalesforceAuthenticationFailed as e:
        sys.exit(f"Salesforce login failed: {e}")

    for name, query in (
        ("Contact", "SELECT Id FROM Contact LIMIT 1"),
        ("Account", "SELECT Id FROM Account LIMIT 1"),
    ):
        result = sf.query(query)
        n = len(result.get("records", []))
        print(f"{name}: sample SOQL OK ({n} row(s) in LIMIT 1)")

    print("Salesforce login OK.")


if __name__ == "__main__":
    main()

"""One-time Salesforce → JSONL snapshot for the sf_pipedrive_snapshot fixture."""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_OUT = (
    Path(__file__).resolve().parents[1]
    / "src" / "aidmi_orchestrator" / "fixtures" / "sf_pipedrive_snapshot" / "source"
)


def main() -> None:
    load_dotenv(override=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=200)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    from aidmi_orchestrator.fixtures.sf_pipedrive.salesforce.fixture_source import (
        _salesforce_client,
    )
    from aidmi_orchestrator.fixtures.sf_pipedrive.salesforce.helpers.records import (
        get_records,
    )

    sf = _salesforce_client()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for sobject, filename in (("Contact", "contact.jsonl"), ("Account", "account.jsonl")):
        rows = list(itertools.islice(get_records(sf, sobject), args.max_rows))
        path = args.out_dir / filename
        with open(path, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, default=str) + "\n")
        print(f"wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()

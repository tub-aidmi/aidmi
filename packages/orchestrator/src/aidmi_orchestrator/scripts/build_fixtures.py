"""Generate v2 Postgres SQL fixtures from canonical-first synthetic CRM data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aidmi_orchestrator.ddl_target_schema import parse_ddl_file
from aidmi_orchestrator.scripts.fixtures_gen.dataset import gen_dataset
from aidmi_orchestrator.scripts.fixtures_gen.destination import (
    DEST_DDL_PG,
    write_destination_pg,
)
from aidmi_orchestrator.scripts.fixtures_gen.sources import (
    write_source_p1_pg,
    write_source_p2_pg,
    write_source_p3_pg,
    write_source_p4_pg,
)

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"

PROBLEM_TO_FIXTURE = {
    "p1": "wrong_field_names_v2",
    "p2": "messy_data_v2",
    "p3": "missing_relations_v2",
    "p4": "master_v2",
}


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


def build_fixture(pkey: str, out_root: Path | None = None) -> dict[str, int]:
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

    out_dir = (out_root or FIXTURES_DIR) / fixture_name
    out_dir.mkdir(parents=True, exist_ok=True)

    source_sql = cfg["writer"](data, source_schema)
    destination_sql = write_destination_pg(data, golden_schema)

    (out_dir / "source.sql").write_text(source_sql, encoding="utf-8")
    (out_dir / "destination.sql").write_text(destination_sql, encoding="utf-8")

    schema = parse_ddl_file(DEST_DDL_PG)
    (out_dir / "target_schema.json").write_text(
        json.dumps(schema.model_dump(exclude_none=True), indent=2, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )

    counts = {k: len(v) for k, v in data.items()}
    print(f"  {fixture_name}: {counts}")
    return counts


def main(out_root: Path | None = None) -> None:
    labels = {
        "p1": "Wrong Field & Object Names",
        "p2": "Messy Data",
        "p3": "Missing / Unclear Relationships",
        "p4": "Master Dataset (all problems)",
    }
    for pkey in ["p1", "p2", "p3", "p4"]:
        print(f"Building {pkey.upper()} — {labels[pkey]} ...")
        build_fixture(pkey, out_root)
    print("Done.")


if __name__ == "__main__":
    main()

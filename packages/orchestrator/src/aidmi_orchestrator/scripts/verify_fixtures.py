"""Regenerate fixture SQL into a temp tree and assert byte-equality with the repo."""

from __future__ import annotations

import filecmp
import shutil
import sys
import tempfile
from pathlib import Path

FIXTURE_DIRS = (
    "master",
    "messy_data",
    "missing_relations",
    "wrong_field_names",
    "master_v2",
    "messy_data_v2",
    "missing_relations_v2",
    "wrong_field_names_v2",
)
GENERATED = ("source.sql", "destination.sql", "target_schema.json")


def main() -> None:
    from aidmi_orchestrator.scripts import build_fixtures

    root = Path(build_fixtures.FIXTURES_DIR)
    with tempfile.TemporaryDirectory() as tmp:
        backup = Path(tmp) / "before"
        backup.mkdir()
        for name in FIXTURE_DIRS:
            src = root / name
            if src.is_dir():
                shutil.copytree(src, backup / name)

        build_fixtures.main()

        drift = []
        for name in FIXTURE_DIRS:
            for fname in GENERATED:
                a, b = backup / name / fname, root / name / fname
                if not a.is_file() and not b.is_file():
                    continue
                if not a.is_file() or not b.is_file():
                    drift.append(f"{name}/{fname}: existence differs")
                elif not filecmp.cmp(a, b, shallow=False):
                    drift.append(f"{name}/{fname}: content differs")

        for name in FIXTURE_DIRS:
            if (backup / name).is_dir():
                shutil.rmtree(root / name, ignore_errors=True)
                shutil.copytree(backup / name, root / name)

    if drift:
        print("FIXTURE DRIFT:")
        for line in drift:
            print(f"  - {line}")
        sys.exit(1)
    print(f"OK — {len(FIXTURE_DIRS)} fixtures regenerate byte-identically")


if __name__ == "__main__":
    main()

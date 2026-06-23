"""Generate target_schema.json from a fixture destination.sql (Postgres DDL)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from aidmi_orchestrator.ddl_target_schema import parse_ddl_file

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def _fixture_paths(name: str) -> tuple[Path, Path]:
    fixture_dir = FIXTURES_DIR / name
    if not fixture_dir.is_dir():
        raise SystemExit(f"unknown fixture {name!r} (no directory at {fixture_dir})")
    src = fixture_dir / "destination.sql"
    if not src.exists():
        raise SystemExit(f"fixture {name!r} has no destination.sql at {src}")
    return src, fixture_dir / "target_schema.json"


def generate(input_path: Path, output_path: Path) -> None:
    ddl = input_path.read_text(encoding="utf-8")
    schema = parse_ddl_file(ddl)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(schema.model_dump(exclude_none=True), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {output_path} ({len(schema.tables)} tables)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fixture", help="fixture name (reads fixtures/<name>/destination.sql)")
    group.add_argument("--input", type=Path, help="path to destination.sql")
    parser.add_argument(
        "--output",
        type=Path,
        help="output path for target_schema.json (required with --input)",
    )
    args = parser.parse_args()

    if args.fixture:
        input_path, output_path = _fixture_paths(args.fixture)
    else:
        if args.output is None:
            raise SystemExit("--output is required with --input")
        input_path, output_path = args.input, args.output

    generate(input_path, output_path)


if __name__ == "__main__":
    main()

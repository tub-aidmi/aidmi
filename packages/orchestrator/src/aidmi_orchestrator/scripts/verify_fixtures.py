"""Regenerate fixture SQL into a temp tree and diff it against a digest baseline.

The gate asserts *generator invariance*: that the generator still emits exactly what
it emitted when the baseline was taken. It deliberately does not compare against the
committed fixture files under `fixtures/` -- those are older than the generator and
answering "are they current?" is a separate product question. Nothing here writes
into the repo's fixture tree.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

BASELINE_PATH = Path("benchmarks/.baseline/fixtures.sha256")

# `gen_date` passes relative strings ("-2y", "+1y") to Faker, which resolves them
# against `date.today()`, so raw generator output shifts by one day every calendar
# day. The gate freezes today so a comparison isolates refactor drift instead of
# elapsed time. Frozen here, in the gate, never in the generator; the value is the
# date the committed fixtures were last regenerated.
FROZEN_TODAY = dt.date(2026, 7, 4)


@contextmanager
def frozen_today() -> Iterator[None]:
    """Resolve Faker's relative date strings against `FROZEN_TODAY`."""
    from faker.providers.date_time import Provider

    original = Provider.__dict__["_parse_date"]

    def _parse_date(cls, value):  # type: ignore[no-untyped-def]
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        if isinstance(value, dt.timedelta):
            return FROZEN_TODAY + value
        if isinstance(value, str):
            if value in ("today", "now"):
                return FROZEN_TODAY
            return FROZEN_TODAY + dt.timedelta(**cls._parse_date_string(value))
        if isinstance(value, int):
            return FROZEN_TODAY + dt.timedelta(value)
        return original.__func__(cls, value)

    Provider._parse_date = classmethod(_parse_date)
    try:
        yield
    finally:
        Provider._parse_date = original


def generate(out_root: Path) -> None:
    from aidmi_orchestrator.scripts import build_fixtures

    with frozen_today():
        build_fixtures.main(out_root)


def guarded_generate(out_root: Path) -> None:
    """Run `generate` and verify it left the repo's committed fixture tree untouched.

    `generate` is only supposed to write into `out_root`; the committed fixtures
    under `build_fixtures.FIXTURES_DIR` are the input data for published benchmark
    campaigns and must never be rewritten by a code path that forgets to thread
    `out_root` through. This digests that tree immediately before and after the
    call and raises if anything changed.
    """
    from aidmi_orchestrator.scripts import build_fixtures

    repo_fixtures = build_fixtures.FIXTURES_DIR
    before = digest_tree(repo_fixtures)
    generate(out_root)
    after = digest_tree(repo_fixtures)
    if before != after:
        changed = sorted(set(before) | set(after))
        offending = next(rel for rel in changed if before.get(rel) != after.get(rel))
        raise RuntimeError(
            "generator wrote into the repo fixture tree instead of out_root: "
            f"{repo_fixtures / offending}"
        )


def digest_tree(root: Path) -> dict[str, str]:
    digests: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            rel = path.relative_to(root).as_posix()
            digests[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
    return digests


def format_manifest(digests: dict[str, str]) -> str:
    return "".join(f"{digests[name]}  {name}\n" for name in sorted(digests))


def parse_manifest(text: str) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        digest, name = line.split("  ", 1)
        manifest[name] = digest
    return manifest


def verify(actual: dict[str, str], baseline_path: Path) -> list[str]:
    if not baseline_path.is_file():
        return [
            f"no baseline at {baseline_path} "
            "(run `just verify-fixtures --snapshot` to create one)"
        ]

    expected = parse_manifest(baseline_path.read_text())
    drift: list[str] = []
    for name in sorted(set(expected) - set(actual)):
        drift.append(f"{name}: in baseline but not regenerated")
    for name in sorted(set(actual) - set(expected)):
        drift.append(f"{name}: regenerated but not in baseline")
    for name in sorted(set(actual) & set(expected)):
        if actual[name] != expected[name]:
            drift.append(
                f"{name}: content differs "
                f"(baseline {expected[name][:12]}, now {actual[name][:12]})"
            )
    return drift


def snapshot(baseline_path: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        guarded_generate(out)
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(format_manifest(digest_tree(out)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot", action="store_true", help="rewrite the digest baseline"
    )
    parser.add_argument(
        "--baseline", type=Path, default=BASELINE_PATH, help="baseline manifest path"
    )
    args = parser.parse_args()

    if args.snapshot:
        snapshot(args.baseline)
        print(f"snapshot -> {args.baseline}")
        return

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        guarded_generate(out)
        drift = verify(digest_tree(out), args.baseline)

    if drift:
        print("FIXTURE DRIFT:")
        for line in drift:
            print(f"  - {line}")
        sys.exit(1)
    print("OK — generator output matches the baseline digests")


if __name__ == "__main__":
    main()

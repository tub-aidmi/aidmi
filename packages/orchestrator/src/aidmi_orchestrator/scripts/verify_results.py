"""Render the publishable campaigns and diff them against their committed reports.

`tidy.csv` and `index.html` are compared byte-for-byte against the campaign's own
`report/` directory -- the artifacts that actually ship. Figures are compared
structurally against the digest manifest under `benchmarks/.baseline/`.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import tempfile
from pathlib import Path

from aidmi_orchestrator.report.data import campaign_labels, load_records
from aidmi_orchestrator.report.driver import build_report

BASELINE_ROOT = Path("benchmarks/.baseline")
CAMPAIGNS = ("v2-final-gemini", "v2-final-ise")
COMPARED_FILES = ("tidy.csv", "index.html")

_DATE_RE = re.compile(r"<dc:date>.*?</dc:date>", re.DOTALL)
_ID_RE = re.compile(r'\b(id|xlink:href|clip-path|href)="[^"]*"')


def normalize_svg(text: str) -> str:
    """Drop the `<dc:date>` element and any `id`/`xlink:href`/`clip-path`/`href`
    attribute values from an SVG, so only its drawable structure is compared.

    On this repo's figures the render is byte-stable across runs, so this
    normalization isn't load-bearing today -- it's a safety margin in case
    that stability regresses (e.g. a future matplotlib bump reintroducing
    randomized ids or timestamps).
    """
    return _ID_RE.sub("", _DATE_RE.sub("", text))


def _hash_svg(text: str) -> str:
    return hashlib.sha256(normalize_svg(text).encode()).hexdigest()


def render(campaign_dir: Path, out_dir: Path) -> None:
    records = load_records([campaign_dir])
    if not records:
        raise SystemExit(f"no result rows under {campaign_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    labels = campaign_labels([campaign_dir])
    build_report(records, out_dir, labels=labels)


def _write_figures_manifest(
    baseline_dir: Path, out_dir: Path, names: list[str]
) -> None:
    lines = []
    for name in names:
        digest = _hash_svg((out_dir / "figures" / name).read_text())
        lines.append(f"{digest}  {name}")
    (baseline_dir / "figures.sha256").write_text("\n".join(lines) + "\n")


def _read_figures_manifest(baseline_dir: Path) -> dict[str, str]:
    manifest_path = baseline_dir / "figures.sha256"
    manifest: dict[str, str] = {}
    for line in manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        digest, name = line.split("  ", 1)
        manifest[name] = digest
    return manifest


def snapshot(campaign_dir: Path, baseline_dir: Path, report_dir: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        render(campaign_dir, out)
        report_dir.mkdir(parents=True, exist_ok=True)
        for name in COMPARED_FILES:
            (report_dir / name).write_bytes((out / name).read_bytes())
        figures_dir = report_dir / "figures"
        figures_dir.mkdir(parents=True, exist_ok=True)
        names = sorted(p.name for p in (out / "figures").glob("*.svg"))
        for name in names:
            (figures_dir / name).write_bytes((out / "figures" / name).read_bytes())
        baseline_dir.mkdir(parents=True, exist_ok=True)
        _write_figures_manifest(baseline_dir, out, names)


def verify(rendered_dir: Path, baseline_dir: Path, report_dir: Path) -> list[str]:
    if not baseline_dir.is_dir():
        return [
            f"no baseline at {baseline_dir} "
            "(run `just snapshot-results` to create one, or check --snapshot usage)"
        ]

    drift: list[str] = []
    for name in COMPARED_FILES:
        committed = report_dir / name
        if not committed.is_file():
            drift.append(
                f"{name} missing from committed report {report_dir} "
                "(run `just snapshot-results` after a reviewed output change)"
            )
        elif committed.read_bytes() != (rendered_dir / name).read_bytes():
            drift.append(f"{name} differs from committed report {report_dir / name}")

    expected_manifest = _read_figures_manifest(baseline_dir)
    expected_names = sorted(expected_manifest)
    actual_names = sorted(p.name for p in (rendered_dir / "figures").glob("*.svg"))
    if expected_names != actual_names:
        missing = sorted(set(expected_names) - set(actual_names))
        added = sorted(set(actual_names) - set(expected_names))
        drift.append(f"figure set differs (missing={missing}, added={added})")

    for name in sorted(set(expected_names) & set(actual_names)):
        actual_digest = _hash_svg((rendered_dir / "figures" / name).read_text())
        if expected_manifest[name] != actual_digest:
            drift.append(f"figures/{name} differs structurally")
    return drift


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", action="store_true", help="rewrite the baseline")
    parser.add_argument("--campaign", action="append", help="campaign dir (repeatable)")
    args = parser.parse_args()

    campaigns = (
        [Path(c) for c in args.campaign]
        if args.campaign
        else [Path("benchmarks") / c for c in CAMPAIGNS]
    )

    failed = False
    for campaign_dir in campaigns:
        baseline_dir = BASELINE_ROOT / campaign_dir.name
        report_dir = campaign_dir / "report"
        if args.snapshot:
            snapshot(campaign_dir, baseline_dir, report_dir)
            print(f"snapshot {campaign_dir.name} -> {report_dir}, {baseline_dir}")
            continue
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            render(campaign_dir, out)
            drift = verify(out, baseline_dir, report_dir)
        if drift:
            failed = True
            print(f"DRIFT {campaign_dir.name}:")
            for line in drift:
                print(f"  - {line}")
        else:
            print(f"OK {campaign_dir.name}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

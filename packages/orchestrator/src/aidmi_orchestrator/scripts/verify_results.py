"""Render the publishable campaigns and diff them against a committed baseline."""
from __future__ import annotations

import argparse
import re
import sys
import tempfile
from pathlib import Path

from aidmi_orchestrator.report.data import load_records
from aidmi_orchestrator.report.driver import build_report

BASELINE_ROOT = Path("benchmarks/.baseline")
CAMPAIGNS = ("2026-07-06-yq3v", "2026-07-07-kw8r")

_DATE_RE = re.compile(r"<dc:date>.*?</dc:date>", re.DOTALL)
_ID_RE = re.compile(r'\b(id|xlink:href|clip-path|href)="[^"]*"')


def normalize_svg(text: str) -> str:
    """Strip the parts matplotlib re-randomizes on every render."""
    return _ID_RE.sub("", _DATE_RE.sub("", text))


def render(campaign_dir: Path, out_dir: Path) -> None:
    records = load_records([campaign_dir])
    if not records:
        raise SystemExit(f"no result rows under {campaign_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    build_report(records, out_dir)


def snapshot(campaign_dir: Path, baseline_dir: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        render(campaign_dir, out)
        baseline_dir.mkdir(parents=True, exist_ok=True)
        (baseline_dir / "tidy.csv").write_text((out / "tidy.csv").read_text())
        (baseline_dir / "index.html").write_text((out / "index.html").read_text())
        names = sorted(p.name for p in (out / "figures").glob("*.svg"))
        (baseline_dir / "figures.txt").write_text("\n".join(names) + "\n")
        for name in names:
            target = baseline_dir / "figures_normalized" / f"{name}.txt"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(normalize_svg((out / "figures" / name).read_text()))


def verify(rendered_dir: Path, baseline_dir: Path) -> list[str]:
    drift: list[str] = []
    for name in ("tidy.csv", "index.html"):
        expected = (baseline_dir / name).read_text()
        actual = (rendered_dir / name).read_text()
        if expected != actual:
            drift.append(f"{name} differs from baseline")

    expected_names = (baseline_dir / "figures.txt").read_text().split()
    actual_names = sorted(p.name for p in (rendered_dir / "figures").glob("*.svg"))
    if expected_names != actual_names:
        missing = sorted(set(expected_names) - set(actual_names))
        added = sorted(set(actual_names) - set(expected_names))
        drift.append(f"figure set differs (missing={missing}, added={added})")

    norm_dir = baseline_dir / "figures_normalized"
    if norm_dir.is_dir():
        for name in sorted(set(expected_names) & set(actual_names)):
            expected_svg = (norm_dir / f"{name}.txt").read_text()
            actual_svg = normalize_svg((rendered_dir / "figures" / name).read_text())
            if expected_svg != actual_svg:
                drift.append(f"figures/{name} differs structurally")
    return drift


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", action="store_true", help="rewrite the baseline")
    parser.add_argument("--campaign", action="append", help="campaign dir (repeatable)")
    args = parser.parse_args()

    campaigns = [Path(c) for c in args.campaign] if args.campaign else [
        Path("benchmarks") / c for c in CAMPAIGNS
    ]

    failed = False
    for campaign_dir in campaigns:
        baseline_dir = BASELINE_ROOT / campaign_dir.name
        if args.snapshot:
            snapshot(campaign_dir, baseline_dir)
            print(f"snapshot {campaign_dir.name} -> {baseline_dir}")
            continue
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            render(campaign_dir, out)
            drift = verify(out, baseline_dir)
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

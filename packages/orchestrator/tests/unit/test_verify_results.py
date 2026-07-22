from pathlib import Path

from aidmi_orchestrator.scripts.verify_results import _hash_svg, normalize_svg, verify


def _write_baseline(
    baseline: Path,
    *,
    tidy: str = "campaign,model\nc1,m1\n",
    html: str = "<html></html>",
    figures: dict[str, str] | None = None,
) -> None:
    baseline.mkdir(parents=True, exist_ok=True)
    (baseline / "tidy.csv").write_text(tidy)
    (baseline / "index.html").write_text(html)
    figures = figures or {}
    lines = [f"{_hash_svg(svg)}  {name}" for name, svg in sorted(figures.items())]
    (baseline / "figures.sha256").write_text("\n".join(lines) + ("\n" if lines else ""))


def _write_rendered(
    rendered: Path,
    *,
    tidy: str = "campaign,model\nc1,m1\n",
    html: str = "<html></html>",
    figures: dict[str, str] | None = None,
) -> None:
    (rendered / "figures").mkdir(parents=True, exist_ok=True)
    (rendered / "tidy.csv").write_text(tidy)
    (rendered / "index.html").write_text(html)
    for name, svg in (figures or {}).items():
        (rendered / "figures" / name).write_text(svg)


def test_normalize_svg_strips_render_noise():
    a = '<svg><dc:date>2026-07-22T10:00:00</dc:date><g id="abc123" clip-path="url(#p1)"/></svg>'
    b = '<svg><dc:date>2026-07-23T11:30:00</dc:date><g id="def456" clip-path="url(#p2)"/></svg>'
    assert normalize_svg(a) == normalize_svg(b)


def test_normalize_svg_keeps_structural_difference():
    a = '<svg><g id="abc"><path d="M0 0 L1 1"/></g></svg>'
    b = '<svg><g id="abc"><path d="M0 0 L9 9"/></g></svg>'
    assert normalize_svg(a) != normalize_svg(b)


def test_verify_reports_tidy_drift(tmp_path: Path):
    baseline = tmp_path / "baseline"
    _write_baseline(baseline)

    rendered = tmp_path / "rendered"
    _write_rendered(rendered, tidy="campaign,model\nc1,DIFFERENT\n")

    drift = verify(rendered, baseline)
    assert any("tidy.csv" in d for d in drift)


def test_verify_reports_html_drift(tmp_path: Path):
    baseline = tmp_path / "baseline"
    _write_baseline(baseline)

    rendered = tmp_path / "rendered"
    _write_rendered(rendered, html="<html><body>changed</body></html>")

    drift = verify(rendered, baseline)
    assert any("index.html" in d for d in drift)


def test_verify_reports_missing_figure(tmp_path: Path):
    svg = '<svg><g id="a"><path d="M0 0 L1 1"/></g></svg>'
    baseline = tmp_path / "baseline"
    _write_baseline(baseline, figures={"a.svg": svg, "b.svg": svg})

    rendered = tmp_path / "rendered"
    _write_rendered(rendered, figures={"a.svg": svg})

    drift = verify(rendered, baseline)
    assert any("missing=['b.svg']" in d for d in drift)
    assert any("added=[]" in d for d in drift)


def test_verify_reports_added_figure(tmp_path: Path):
    svg = '<svg><g id="a"><path d="M0 0 L1 1"/></g></svg>'
    baseline = tmp_path / "baseline"
    _write_baseline(baseline, figures={"a.svg": svg})

    rendered = tmp_path / "rendered"
    _write_rendered(rendered, figures={"a.svg": svg, "c.svg": svg})

    drift = verify(rendered, baseline)
    assert any("added=['c.svg']" in d for d in drift)
    assert any("missing=[]" in d for d in drift)


def test_verify_reports_figure_content_drift(tmp_path: Path):
    baseline = tmp_path / "baseline"
    _write_baseline(
        baseline, figures={"a.svg": '<svg><g id="a"><path d="M0 0 L1 1"/></g></svg>'}
    )

    rendered = tmp_path / "rendered"
    _write_rendered(
        rendered, figures={"a.svg": '<svg><g id="a"><path d="M0 0 L9 9"/></g></svg>'}
    )

    drift = verify(rendered, baseline)
    assert any("figures/a.svg" in d and "differs structurally" in d for d in drift)


def test_verify_clean_render_returns_no_drift(tmp_path: Path):
    svg_a = '<svg><g id="a"><path d="M0 0 L1 1"/></g></svg>'
    svg_b = '<svg><g id="b" clip-path="url(#p1)"><path d="M2 2 L3 3"/></g></svg>'
    baseline = tmp_path / "baseline"
    _write_baseline(baseline, figures={"a.svg": svg_a, "b.svg": svg_b})

    rendered = tmp_path / "rendered"
    _write_rendered(rendered, figures={"a.svg": svg_a, "b.svg": svg_b})

    assert verify(rendered, baseline) == []


def test_verify_missing_baseline_dir_reports_actionable_drift(tmp_path: Path):
    rendered = tmp_path / "rendered"
    _write_rendered(rendered)

    baseline = tmp_path / "no-such-baseline"
    drift = verify(rendered, baseline)

    assert len(drift) == 1
    assert "--snapshot" in drift[0]
    assert str(baseline) in drift[0]

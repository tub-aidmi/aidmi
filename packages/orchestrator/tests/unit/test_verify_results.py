from pathlib import Path

from aidmi_orchestrator.scripts.verify_results import normalize_svg, verify


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
    baseline.mkdir()
    (baseline / "tidy.csv").write_text("campaign,model\nc1,m1\n")
    (baseline / "index.html").write_text("<html></html>")
    (baseline / "figures.txt").write_text("")

    rendered = tmp_path / "rendered"
    (rendered / "figures").mkdir(parents=True)
    (rendered / "tidy.csv").write_text("campaign,model\nc1,DIFFERENT\n")
    (rendered / "index.html").write_text("<html></html>")

    drift = verify(rendered, baseline)
    assert any("tidy.csv" in d for d in drift)

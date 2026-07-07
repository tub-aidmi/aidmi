from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.html import Section, render_gallery


def _sections(*, with_cross_campaign: bool) -> list[Section]:
    secs = [
        Section(
            id="headline",
            title="Headline",
            figures=[Path("/abs/somewhere/figures/pareto.svg")],
            caption="Recall vs cost trade-off across configs.",
            table_keys=("best_config",),
        ),
        Section(
            id="metric",
            title="Metric choice",
            figures=[Path("prec_recall.svg")],
            caption="Recall is the primary metric.",
        ),
        Section(
            id="levers",
            title="Levers",
            figures=[Path("lever_sc.svg"), Path("lever_ctx.svg")],
            caption="Self-correction improves recall most.",
        ),
        Section(
            id="strategy",
            title="Strategy",
            figures=[Path("scorecard.svg")],
            caption="Ensemble voting dominates on recall.",
        ),
        Section(
            id="reliability",
            title="Reliability",
            figures=[Path("rep_spread.svg")],
            caption="Silent failures cluster in one cell.",
            table_keys=("silent_failure",),
        ),
        Section(
            id="fixtures",
            title="Fixtures",
            figures=[],
            caption="Fixture difficulty varies widely.",
        ),
    ]
    if with_cross_campaign:
        secs.append(
            Section(
                id="cross_campaign",
                title="Cross-campaign",
                figures=[Path("cross_model.svg")],
                caption="Model choice shifts the frontier.",
            )
        )
    secs.append(
        Section(
            id="appendix",
            title="Appendix",
            figures=[],
            caption="Full per-config breakdown.",
            table_keys=("appendix",),
        )
    )
    return secs


TABLES = {
    "best_config": "<table id='bc'><tr><td>best-config-marker</td></tr></table>",
    "silent_failure": "<table id='sf'><tr><td>silent-fail-marker</td></tr></table>",
    "appendix": "<table id='ap'><tr><td>appendix-marker</td></tr></table>",
}


def test_render_gallery_contains_nav_and_anchors():
    html = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=False),
        tables=TABLES,
        multi_model=False,
    )
    assert "<nav" in html
    for sec_id in ("headline", "metric", "levers", "strategy", "reliability", "fixtures", "appendix"):
        assert f'id="{sec_id}"' in html
        assert f'href="#{sec_id}"' in html


def test_render_gallery_inlines_tables_verbatim():
    html = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=False),
        tables=TABLES,
        multi_model=False,
    )
    assert TABLES["best_config"] in html
    assert TABLES["silent_failure"] in html
    assert TABLES["appendix"] in html


def test_cross_campaign_nav_entry_present_only_when_multi_model():
    with_multi = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=True),
        tables=TABLES,
        multi_model=True,
    )
    without_multi = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=True),
        tables=TABLES,
        multi_model=False,
    )
    assert "Cross-campaign" in with_multi
    assert 'href="#cross_campaign"' in with_multi
    assert "Cross-campaign" not in without_multi
    assert 'href="#cross_campaign"' not in without_multi
    assert 'id="cross_campaign"' not in without_multi


def test_render_gallery_is_self_contained_html_document():
    html = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=False),
        tables=TABLES,
        multi_model=False,
    )
    assert "<style>" in html
    assert "http://" not in html
    assert "https://" not in html
    assert "<script" not in html


def test_figure_src_is_relative_to_figures_dir_using_basename():
    html = render_gallery(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=False),
        tables=TABLES,
        multi_model=False,
    )
    # absolute source path is reduced to its basename under figures/
    assert 'src="figures/pareto.svg"' in html
    assert "/abs/somewhere/figures/pareto.svg" not in html
    assert 'src="figures/lever_sc.svg"' in html


def test_section_title_and_caption_are_escaped():
    secs = _sections(with_cross_campaign=False)
    secs[0] = Section(
        id="headline",
        title="Headline <script>alert(1)</script>",
        figures=[],
        caption="Cost < recall & <b>bold</b> claim",
        table_keys=("best_config",),
    )
    html = render_gallery(
        title="Benchmark exploration",
        sections=secs,
        tables=TABLES,
        multi_model=False,
    )
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
    assert "Cost &lt; recall &amp; &lt;b&gt;bold&lt;/b&gt; claim" in html


def test_render_gallery_deterministic():
    kwargs = dict(
        title="Benchmark exploration",
        sections=_sections(with_cross_campaign=True),
        tables=TABLES,
        multi_model=True,
    )
    assert render_gallery(**kwargs) == render_gallery(**kwargs)

"""Report driver: assembles all figures + tables into the gallery."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.data import RunRecord, write_tidy_csv
from aidmi_orchestrator.report.figures.context import fig_ctx_comparison
from aidmi_orchestrator.report.figures.correlation import (
    fig_recall_field_acc,
    fig_recall_mat_rate,
    fig_tokens_vs_field_acc,
    fig_tokens_vs_mat_rate,
    fig_tokens_vs_recall,
)
from aidmi_orchestrator.report.figures.distribution import (
    fig_dist_by_fixture,
    fig_dist_by_fixture_for_strategy,
    fig_dist_by_strategy,
    fig_dist_by_strategy_for_fixture,
)
from aidmi_orchestrator.report.figures.efficiency import (
    fig_cost_drivers,
    fig_efficiency,
)
from aidmi_orchestrator.report.figures.heatmap import fig_heatmap, fig_metric_heatmap
from aidmi_orchestrator.report.figures.levers import (
    fig_lever_ctx,
    fig_lever_ctx_sc_on,
    fig_lever_sc,
)
from aidmi_orchestrator.report.figures.metric import fig_prec_recall
from aidmi_orchestrator.report.figures.pareto import fig_pareto
from aidmi_orchestrator.report.figures.reliability import fig_rep_range, fig_rep_spread
from aidmi_orchestrator.report.figures.strategy import fig_cost_latency, fig_scorecard
from aidmi_orchestrator.report.figures.tokens import fig_thinking_tokens
from aidmi_orchestrator.report.html import Section, Subsection, render_gallery
from aidmi_orchestrator.report.tables import (
    appendix_table,
    best_config_table,
    failure_accounting_table,
    silent_failure_table,
    summary_best_config_table,
    summary_by_ctx_table,
    summary_by_sc_table,
    summary_overall_table,
    summary_sc_block,
)
from aidmi_orchestrator.report.theme import apply_theme


def _build_core_figures(records: list[RunRecord], figdir: Path) -> dict[str, Path]:
    """Generate the figures shared by every section, keyed by role."""
    return {
        "pareto": fig_pareto(records, figdir),
        "prec_recall": fig_prec_recall(records, figdir),
        "recall_field_acc": fig_recall_field_acc(records, figdir),
        "recall_mat_rate": fig_recall_mat_rate(records, figdir),
        "corr_tokens_recall": fig_tokens_vs_recall(records, figdir),
        "corr_tokens_field_acc": fig_tokens_vs_field_acc(records, figdir),
        "corr_tokens_mat_rate": fig_tokens_vs_mat_rate(records, figdir),
        "dist_by_strategy": fig_dist_by_strategy(records, figdir),
        "dist_by_fixture": fig_dist_by_fixture(records, figdir),
        "lever_sc": fig_lever_sc(records, figdir),
        "lever_ctx": fig_lever_ctx(records, figdir),
        "lever_ctx_sc_on": fig_lever_ctx_sc_on(records, figdir),
        "ctx_comparison": fig_ctx_comparison(records, figdir),
        "scorecard": fig_scorecard(records, figdir),
        "cost_latency": fig_cost_latency(records, figdir),
        "thinking_tokens": fig_thinking_tokens(records, figdir),
        "efficiency": fig_efficiency(records, figdir),
        "cost_drivers": fig_cost_drivers(records, figdir),
        "rep_spread": fig_rep_spread(records, figdir),
        "rep_range": fig_rep_range(records, figdir),
        **{
            f"heatmap_{key}": fig_metric_heatmap(
                [r for r in records if r.sc is True], figdir, key=key
            )
            for key in ("recall", "field_acc", "mat_rate", "cost", "tokens", "time")
        },
    }


def _build_dist_facets(
    records: list[RunRecord], figdir: Path
) -> tuple[list[Subsection], list[Subsection]]:
    """Per-fixture strategy distributions and per-strategy fixture distributions,
    each restricted to self-correction-on runs, keyed by the facet value."""
    on = [r for r in records if r.sc is True]
    fixtures = sorted({r.fixture for r in on})
    cells = sorted({r.cell for r in on})
    by_fixture = [
        Subsection(fx, [fig_dist_by_strategy_for_fixture(records, figdir, fx)])
        for fx in fixtures
    ]
    by_strategy = [
        Subsection(c, [fig_dist_by_fixture_for_strategy(records, figdir, c)])
        for c in cells
    ]
    return by_fixture, by_strategy


def _build_per_model_heatmaps(records: list[RunRecord], figdir: Path) -> list[Path]:
    """Per-model materialization heatmaps, one per model, for the Cross-campaign section."""
    paths = []
    for model in sorted({r.model for r in records}):
        model_records = [r for r in records if r.model == model]
        paths.append(
            fig_heatmap(
                model_records, figdir,
                metric="materialized", filename=f"heatmap_materialized_{model}.svg",
                title=f"Materialization % — {model}",
            )
        )
    return paths


def _build_sections(
    figs: dict[str, Path],
    per_model_heatmaps: list[Path],
    strategy_by_fixture: list[Subsection],
    fixture_by_strategy: list[Subsection],
    *,
    multi_model: bool,
) -> list[Section]:
    sections = [
        Section(
            "summary", "Summary", [], "",
            ("summary_overall", "summary_sc", "summary_ctx",
             "summary_best_config", "summary_sc_on", "summary_sc_off"),
        ),
        Section(
            "levers", "Levers",
            [figs["lever_ctx"], figs["lever_ctx_sc_on"], figs["lever_sc"]],
            "",
        ),
        Section(
            "correlation", "Correlation",
            [figs["recall_field_acc"], figs["recall_mat_rate"]], "",
            subsections=(
                Subsection(
                    "Correlation with total token usage (self-correction on)",
                    [figs["corr_tokens_recall"], figs["corr_tokens_field_acc"],
                     figs["corr_tokens_mat_rate"]],
                ),
            ),
        ),
        Section(
            "heatmaps", "Heatmaps",
            # Row-major 2-column flow: quality (recall, field acc, mat rate) down
            # the left, cost/effort (cost, tokens, time) down the right.
            [figs["heatmap_recall"], figs["heatmap_cost"],
             figs["heatmap_field_acc"], figs["heatmap_tokens"],
             figs["heatmap_mat_rate"], figs["heatmap_time"]],
            "",
        ),
        Section(
            "distribution", "Distribution",
            [figs["dist_by_strategy"], figs["dist_by_fixture"]],
            "",
            stacked=True,
        ),
        Section(
            "strategy_by_fixture", "Strategy by fixture", [], "",
            stacked=True, subsections=tuple(strategy_by_fixture),
        ),
        Section(
            "fixture_by_strategy", "Fixture by strategy", [], "",
            stacked=True, subsections=tuple(fixture_by_strategy),
        ),
        Section(
            "efficiency", "Efficiency",
            [figs["efficiency"], figs["thinking_tokens"], figs["cost_drivers"]],
            "",
        ),
    ]
    if multi_model:
        sections.append(
            Section(
                "cross_campaign", "Cross-campaign", per_model_heatmaps, "",
            )
        )
    return sections


def build_report(records: list[RunRecord], out_dir: Path) -> list[Path]:
    apply_theme()
    out_dir = Path(out_dir)
    figdir = out_dir / "figures"
    figdir.mkdir(parents=True, exist_ok=True)

    multi_model = len({r.model for r in records}) > 1

    figs = _build_core_figures(records, figdir)
    per_model_heatmaps = _build_per_model_heatmaps(records, figdir) if multi_model else []
    strategy_by_fixture, fixture_by_strategy = _build_dist_facets(records, figdir)

    tables = {
        "summary_overall": summary_overall_table(records),
        "summary_sc": summary_by_sc_table(records),
        "summary_ctx": summary_by_ctx_table(records),
        "summary_best_config": summary_best_config_table(records),
        "summary_sc_on": summary_sc_block(records, sc=True),
        "summary_sc_off": summary_sc_block(records, sc=False),
        "best_config": best_config_table(records),
        "failure_accounting": failure_accounting_table(records),
        "silent_failure": silent_failure_table(records),
        "appendix": appendix_table(records),
    }

    sections = _build_sections(
        figs, per_model_heatmaps, strategy_by_fixture, fixture_by_strategy,
        multi_model=multi_model,
    )

    campaigns = sorted({r.campaign for r in records})
    title = f"aidmi benchmark report — {', '.join(campaigns)}"

    html = render_gallery(title=title, sections=sections, tables=tables, multi_model=multi_model)
    index_path = out_dir / "index.html"
    index_path.write_text(html)

    tidy_path = out_dir / "tidy.csv"
    write_tidy_csv(records, tidy_path)

    facet_paths = [
        f for sub in (*strategy_by_fixture, *fixture_by_strategy) for f in sub.figures
    ]
    written = (
        list(figs.values()) + per_model_heatmaps + facet_paths
        + [index_path, tidy_path]
    )
    return written

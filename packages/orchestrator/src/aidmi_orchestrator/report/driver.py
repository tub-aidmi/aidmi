"""Report driver: assembles all figures + tables into the gallery."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.data import RunRecord, write_tidy_csv
from aidmi_orchestrator.report.figures.context import fig_ctx_comparison
from aidmi_orchestrator.report.figures.distribution import (
    fig_metric_distribution,
    fig_score_histogram,
)
from aidmi_orchestrator.report.figures.efficiency import (
    fig_cost_drivers,
    fig_efficiency,
)
from aidmi_orchestrator.report.figures.heatmap import fig_heatmap, fig_std_heatmap
from aidmi_orchestrator.report.figures.levers import fig_lever_ctx, fig_lever_sc
from aidmi_orchestrator.report.figures.metric import fig_prec_recall
from aidmi_orchestrator.report.figures.pareto import fig_pareto
from aidmi_orchestrator.report.figures.reliability import fig_rep_range, fig_rep_spread
from aidmi_orchestrator.report.figures.strategy import fig_cost_latency, fig_scorecard
from aidmi_orchestrator.report.figures.tokens import fig_thinking_tokens
from aidmi_orchestrator.report.html import Section, render_gallery
from aidmi_orchestrator.report.tables import (
    appendix_table,
    best_config_table,
    failure_accounting_table,
    silent_failure_table,
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
        "metric_distribution": fig_metric_distribution(records, figdir),
        "score_histogram": fig_score_histogram(records, figdir),
        "lever_sc": fig_lever_sc(records, figdir),
        "lever_ctx": fig_lever_ctx(records, figdir),
        "ctx_comparison": fig_ctx_comparison(records, figdir),
        "scorecard": fig_scorecard(records, figdir),
        "cost_latency": fig_cost_latency(records, figdir),
        "thinking_tokens": fig_thinking_tokens(records, figdir),
        "efficiency": fig_efficiency(records, figdir),
        "cost_drivers": fig_cost_drivers(records, figdir),
        "rep_spread": fig_rep_spread(records, figdir),
        "rep_range": fig_rep_range(records, figdir),
        "heatmap_f1_std": fig_std_heatmap(records, figdir),
        "heatmap_materialized": fig_heatmap(
            records, figdir,
            metric="materialized", filename="heatmap_materialized.svg",
            title="Materialization % by strategy × fixture",
        ),
        "heatmap_field_acc": fig_heatmap(
            records, figdir,
            metric="field_acc", filename="heatmap_field_acc.svg",
            title="Field accuracy by strategy × fixture",
        ),
    }


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
    figs: dict[str, Path], per_model_heatmaps: list[Path], *, multi_model: bool
) -> list[Section]:
    sections = [
        Section(
            "summary", "Summary", [],
            "Run totals with mean / median / sd for the headline metrics — overall, "
            "split by the two dominant levers (self-correction, context mode), then "
            "per-strategy and per-fixture breakdowns within each self-correction setting.",
            ("summary_overall", "summary_sc", "summary_ctx",
             "summary_sc_on", "summary_sc_off"),
        ),
        Section(
            "distribution", "Distribution",
            [figs["metric_distribution"], figs["score_histogram"]],
            "Scores are bimodal — runs pile at 0 and 1, not the mean. The box + raw dots expose the spread a mean bar hides.",
        ),
        Section(
            "levers", "Levers",
            [figs["lever_sc"], figs["lever_ctx"], figs["ctx_comparison"]],
            "Self-correction is the dominant lever; live_query_tool spends ~2.5× the input tokens of metadata_only for a fraction of a point of f1.",
        ),
        Section(
            "strategy", "Strategy", [figs["scorecard"], figs["cost_latency"]],
            "Per-strategy materialization, recall, field accuracy, and their cost/latency trade-offs.",
        ),
        Section(
            "efficiency", "Efficiency",
            [figs["efficiency"], figs["thinking_tokens"], figs["cost_drivers"]],
            "Resource per unit of quality (cost/f1, tokens/f1), the reasoning-token tax, and the retry/cache drivers behind cost.",
        ),
        Section(
            "fixtures", "Fixtures",
            [figs["heatmap_materialized"], figs["heatmap_field_acc"], figs["heatmap_f1_std"]],
            "Materialization and field accuracy decouple across fixtures; the std heatmap flags cells whose mean you shouldn't trust.",
        ),
    ]
    if multi_model:
        sections.append(
            Section(
                "cross_campaign", "Cross-campaign", per_model_heatmaps,
                "Per-model fixture materialization; the figures above are already faceted by model.",
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

    tables = {
        "summary_overall": summary_overall_table(records),
        "summary_sc": summary_by_sc_table(records),
        "summary_ctx": summary_by_ctx_table(records),
        "summary_sc_on": summary_sc_block(records, sc=True),
        "summary_sc_off": summary_sc_block(records, sc=False),
        "best_config": best_config_table(records),
        "failure_accounting": failure_accounting_table(records),
        "silent_failure": silent_failure_table(records),
        "appendix": appendix_table(records),
    }

    sections = _build_sections(figs, per_model_heatmaps, multi_model=multi_model)

    campaigns = sorted({r.campaign for r in records})
    title = f"aidmi benchmark report — {', '.join(campaigns)}"

    html = render_gallery(title=title, sections=sections, tables=tables, multi_model=multi_model)
    index_path = out_dir / "index.html"
    index_path.write_text(html)

    tidy_path = out_dir / "tidy.csv"
    write_tidy_csv(records, tidy_path)

    written = list(figs.values()) + per_model_heatmaps + [index_path, tidy_path]
    return written

"""Grouped bar plot rendering."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.format import mean_plot_title, uniform_plot_n
from aidmi_orchestrator.report.plot_specs import GroupedBarPlotSpec

_PLOT_TITLES = {
    "preservation_profile": "Preservation profile",
    "schema_errors": "Schema errors",
    "tokens_in_out": "Input vs output tokens",
    "preservation_per_table": "Per-table row ratio",
}


def _require_matplotlib():
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def render_grouped_bar_svg(spec: GroupedBarPlotSpec, svg_path: Path) -> None:
    plt = _require_matplotlib()
    n_models = len(spec.col_labels)
    n_series = len(spec.series_labels)
    if n_models == 0 or n_series == 0:
        return

    width = 0.8 / n_series
    x = list(range(n_models))
    fig, ax = plt.subplots(
        figsize=(max(6.0, n_models * 1.4), 4.5),
        constrained_layout=True,
    )
    for i, label in enumerate(spec.series_labels):
        offset = (i - (n_series - 1) / 2) * width
        positions = [xi + offset for xi in x]
        heights = spec.values[i]
        ax.bar(positions, heights, width=width, label=label)

    ax.set_xticks(x)
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    title_label = _PLOT_TITLES.get(spec.plot_id, spec.plot_id)
    context = f"{spec.strategy} — {spec.fixture}"
    plot_n = uniform_plot_n(spec.n_by_model)
    ax.set_title(mean_plot_title(title_label, context, plot_n))
    ax.legend(title="Series", loc="upper right")

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)

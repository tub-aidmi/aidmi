"""Outcome funnel plot rendering."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.format import uniform_plot_n
from aidmi_orchestrator.report.plot_specs import FunnelPlotSpec


def render_funnel_svg(spec: FunnelPlotSpec, svg_path: Path) -> None:
    try:
        import matplotlib
    except ImportError as e:
        raise RuntimeError(
            "matplotlib is not installed — install the plots extra: "
            "uv sync --extra plots (or pip install 'aidmi-orchestrator[plots]')"
        ) from e
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n_models = len(spec.col_labels)
    n_stages = len(spec.stage_labels)
    if n_models == 0 or n_stages == 0:
        return

    width = 0.8 / n_stages
    x = list(range(n_models))
    fig, ax = plt.subplots(
        figsize=(max(6.0, n_models * 1.4), 4.5),
        constrained_layout=True,
    )
    for i, stage in enumerate(spec.stage_labels):
        offset = (i - (n_stages - 1) / 2) * width
        positions = [xi + offset for xi in x]
        heights = spec.pass_rates[i]
        ax.bar(positions, heights, width=width, label=stage)

    ax.set_ylim(0, 1.05)
    ax.set_xticks(x)
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    ax.set_ylabel("Pass rate")
    plot_n = uniform_plot_n(spec.n_by_model)
    suffix = f" (n={plot_n})" if plot_n and plot_n > 1 else ""
    ax.set_title(f"Outcome funnel — {spec.strategy} — {spec.fixture}{suffix}")
    ax.legend(title="Stage", loc="upper right")

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)

"""Stacked bar plot rendering for per-role metrics."""
from __future__ import annotations

from pathlib import Path

from aidmi_orchestrator.report.format import mean_plot_title, uniform_plot_n
from aidmi_orchestrator.report.role_aggregate import RoleStackedBarSpec

_METRIC_LABELS = {
    "tokens_input_by_role": "input tokens",
    "tokens_output_by_role": "output tokens",
    "llm_calls_by_role": "LLM calls",
    "latency_ms_sum_by_role": "latency (ms)",
}

_ROLE_COLORS = {
    "writer": "#4C78A8",
    "planner": "#F58518",
    "critic": "#E45756",
    "judge": "#72B7B2",
}


def _role_color(role: str, index: int) -> str:
    if role in _ROLE_COLORS:
        return _ROLE_COLORS[role]
    palette = ["#54A24B", "#EECA3B", "#B279A2", "#FF9DA6", "#9D755D"]
    return palette[index % len(palette)]


def _format_total(value: float, metric: str) -> str:
    if metric in ("llm_calls_by_role",):
        if abs(value - round(value)) < 1e-6:
            return f"{int(round(value))}"
        return f"{value:.1f}"
    if metric == "latency_ms_sum_by_role":
        if value >= 1000:
            return f"{value:.3g}"
        return f"{value:.0f}"
    if value >= 1000:
        return f"{value:.3g}"
    return f"{value:.0f}"


def render_stacked_bar_svg(spec: RoleStackedBarSpec, svg_path: Path) -> None:
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
    if n_models == 0:
        return

    metric_label = _METRIC_LABELS.get(spec.metric, spec.metric)
    fig, ax = plt.subplots(
        figsize=(max(6.0, n_models * 1.4), 4.5),
        constrained_layout=True,
    )

    x = range(n_models)
    bottoms = [0.0] * n_models
    for role_index, role in enumerate(spec.role_labels):
        heights = [seg.get(role, 0.0) for seg in spec.segments]
        color = _role_color(role, role_index)
        ax.bar(x, heights, bottom=bottoms, label=role, color=color, width=0.7)
        bottoms = [b + h for b, h in zip(bottoms, heights)]

    ymax = max(spec.totals) if spec.totals else 1.0
    if ymax == 0:
        ymax = 1.0
    ax.set_ylim(0, ymax * 1.12)

    for i, total in enumerate(spec.totals):
        if total > 0:
            ax.text(
                i, total, _format_total(total, spec.metric),
                ha="center", va="bottom", fontsize=9,
            )

    ax.set_xticks(list(x))
    ax.set_xticklabels(spec.col_labels, rotation=35, ha="right")
    ax.set_xlabel("Model")
    ax.set_ylabel(metric_label)
    context = f"{spec.strategy} — {spec.fixture}"
    plot_n = uniform_plot_n(spec.n_by_model)
    ax.set_title(mean_plot_title(f"{metric_label} by role", context, plot_n))
    ax.legend(title="Role", loc="upper right")

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(svg_path, format="svg", bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)

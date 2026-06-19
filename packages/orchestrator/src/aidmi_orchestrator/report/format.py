"""Shared formatting for report tables and plots."""
from __future__ import annotations


def fmt_agg(agg: dict[str, float] | None) -> str:
    if agg is None:
        return "-"
    if agg["std"]:
        return f"{agg['mean']:.3g}±{agg['std']:.2g}"
    return f"{agg['mean']:.4g}"


def fmt_mean_std(
    mean: float,
    std: float,
    *,
    n: int = 0,
    rate: bool = False,
    metric: str = "",
) -> tuple[str, str | None]:
    if rate:
        mean_line = f"{mean:.2f}"
    elif mean >= 1000:
        mean_line = f"{mean:.3g}"
    elif abs(mean) < 10 and metric not in (
        "tokens_input_total", "tokens_output_total", "wall_clock_seconds", "llm_calls_total",
    ):
        mean_line = f"{mean:.2f}"
    else:
        mean_line = f"{mean:.3g}"

    if n <= 1 or not std:
        return mean_line, None
    return mean_line, f"±{std:.2g}"


def uniform_plot_n(n_values: list[int]) -> int | None:
    if not n_values:
        return None
    unique = set(n_values)
    if len(unique) == 1:
        return next(iter(unique))
    return None


def mean_plot_title(metric_label: str, context: str, n: int | None) -> str:
    prefix = "" if n == 1 else "Mean "
    suffix = f" (n={n})" if n and n > 1 else ""
    return f"{prefix}{metric_label}{suffix} — {context}"

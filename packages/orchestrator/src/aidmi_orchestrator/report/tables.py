"""HTML report tables: best-config, silent-failure, and per-config appendix."""
from __future__ import annotations

import html
import statistics

from collections import Counter

from aidmi_orchestrator.report.aggregate import (
    group_mean,
    group_mean_zero,
    materialization_rate,
    rep_values,
    summary_stats,
)
from aidmi_orchestrator.report.data import RunRecord


def _esc(value) -> str:
    return html.escape(str(value))


def _fmt_sc(sc: bool | None) -> str:
    if sc is None:
        return "n/a"
    return "on" if sc else "off"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _fmt_rate3(value: float | None) -> str:
    return "-" if value is None else f"{value:.3f}"


def _fmt_mean_sd(values: list[float]) -> str:
    if not values:
        return "-"
    mean = statistics.mean(values)
    if len(values) < 2:
        return f"{mean:.3f}"
    sd = statistics.pstdev(values)
    return f"{mean:.3f}±{sd:.3f}"


def _config_key(r: RunRecord) -> tuple[str, str, bool | None, str]:
    return (r.cell, r.ctx, r.sc, r.model)


def _appendix_key(r: RunRecord) -> tuple[str, str, str, bool | None]:
    return (r.model, r.cell, r.ctx, r.sc)


def _norm(key: tuple) -> tuple:
    """All-string, None-safe comparable form of a config tuple for stable ordering."""
    return tuple("" if x is None else str(x) for x in key)


def _best(items: dict, *, largest: bool):
    """Pick the extremal (key, value) with a deterministic tie-break on the config tuple."""
    pick = max if largest else min
    return pick(items.items(), key=lambda kv: (kv[1], _norm(kv[0])))


def _row(cells: list[str]) -> str:
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


def _header(cells: list[str]) -> str:
    return "<tr>" + "".join(f"<th>{c}</th>" for c in cells) + "</tr>"


def _table(header_cells: list[str], body_rows: list[str], caption: str | None = None) -> str:
    caption_html = f"<caption>{caption}</caption>" if caption else ""
    return (
        "<table>"
        + caption_html
        + "<thead>"
        + _header(header_cells)
        + "</thead><tbody>"
        + "".join(body_rows)
        + "</tbody></table>"
    )


def best_config_table(records: list[RunRecord]) -> str:
    recall_by_config = group_mean(records, _config_key, lambda r: r.recall)
    cost_by_config = group_mean(records, _config_key, lambda r: r.cost)
    mat_by_config = materialization_rate(records, _config_key)

    header = ["Objective", "Cell", "Context", "Self-correct", "Model", "Value"]
    rows: list[str] = []

    if recall_by_config:
        (cell, ctx, sc, model), value = _best(recall_by_config, largest=True)
        rows.append(_row([
            "Highest mean recall", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            _fmt_rate3(value),
        ]))

    if cost_by_config:
        (cell, ctx, sc, model), value = _best(cost_by_config, largest=False)
        rows.append(_row([
            "Lowest mean cost", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            f"${value:.4f}",
        ]))

    if mat_by_config:
        (cell, ctx, sc, model), value = _best(mat_by_config, largest=True)
        rows.append(_row([
            "Highest materialization pass-rate", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            _fmt_pct(value),
        ]))

    return _table(header, rows)


_SUMMARY_HEADER = [
    "Group", "n", "Recall", "Field acc", "Mat% (any)", "Mat% (all)",
    "Cost $", "Time (s)",
]
_SUMMARY_LEGEND = (
    "Cells: mean / median ±sd. Recall counts a run that produced nothing as a "
    "genuine 0-of-N score; field acc, cost and time are over runs that produced "
    "output. Mat% (any) is the share of runs that materialized at least one "
    "target table; Mat% (all) the share that materialized every target table."
)


def _zero_vals(recs: list[RunRecord], metric) -> list[float]:
    return [v if (v := metric(r)) is not None else 0.0 for r in recs]


def _eval_vals(recs: list[RunRecord], metric) -> list[float]:
    return [v for r in recs if (v := metric(r)) is not None]


def _fmt_stats(values: list[float], *, prec: int = 3, prefix: str = "",
               integer: bool = False) -> str:
    stats = summary_stats(values)
    if stats is None:
        return "-"
    mean, median, sd = stats
    if integer:
        return f"{prefix}{mean:.0f} / {median:.0f} ±{sd:.0f}"
    return f"{prefix}{mean:.{prec}f} / {median:.{prec}f} ±{sd:.{prec}f}"


def _summary_row(label: str, recs: list[RunRecord]) -> str:
    n = len(recs)
    mat_any = sum(1 for r in recs if r.materialized) / n if n else 0.0
    mat_all = sum(1 for r in recs if r.tables_materialized == 1.0) / n if n else 0.0
    return _row([
        _esc(label), _esc(n),
        _fmt_stats(_zero_vals(recs, lambda r: r.recall)),
        _fmt_stats(_eval_vals(recs, lambda r: r.field_acc)),
        _fmt_pct(mat_any), _fmt_pct(mat_all),
        _fmt_stats(_eval_vals(recs, lambda r: r.cost), prec=4, prefix="$"),
        _fmt_stats(_eval_vals(recs, lambda r: r.secs), integer=True),
    ])


def _summary_table(groups: list[tuple[str, list[RunRecord]]], caption: str) -> str:
    rows = [_summary_row(label, recs) for label, recs in groups if recs]
    return _table(_SUMMARY_HEADER, rows, caption=caption)


def _ctx_order(records: list[RunRecord]) -> list[str | None]:
    return sorted({r.ctx for r in records}, key=lambda c: (c is None, c or ""))


def summary_overall_table(records: list[RunRecord]) -> str:
    return _summary_table(
        [("All runs", records)],
        caption="Overall run totals across every config. " + _SUMMARY_LEGEND,
    )


def summary_by_sc_table(records: list[RunRecord]) -> str:
    groups = [(_fmt_sc(sc), [r for r in records if r.sc is sc])
              for sc in (True, False)]
    return _summary_table(
        groups, caption="Split by self-correction. " + _SUMMARY_LEGEND)


def summary_by_ctx_table(records: list[RunRecord]) -> str:
    groups = [((ctx or "n/a"), [r for r in records if r.ctx == ctx])
              for ctx in _ctx_order(records)]
    return _summary_table(
        groups,
        caption="Split by context mode — pooled and descriptive only; the effect "
        "flips per strategy (see Levers), so no row is a universal winner. "
        + _SUMMARY_LEGEND,
    )


def _mean_recall(recs: list[RunRecord]) -> float:
    vals = _zero_vals(recs, lambda r: r.recall)
    return sum(vals) / len(vals) if vals else 0.0


def summary_by_strategy_table(records: list[RunRecord]) -> str:
    on = [r for r in records if r.sc is True]
    groups = [(cell, [r for r in on if r.cell == cell])
              for cell in sorted({r.cell for r in on})]
    groups.sort(key=lambda g: (-_mean_recall(g[1]), g[0]))
    return _summary_table(
        groups,
        caption="Per strategy — self-correction on only, both context modes "
        "pooled (context makes little overall difference). Ordered by mean "
        "recall. " + _SUMMARY_LEGEND,
    )


def summary_by_fixture_table(records: list[RunRecord]) -> str:
    on = [r for r in records if r.sc is True]
    groups = [(fixture, [r for r in on if r.fixture == fixture])
              for fixture in sorted({r.fixture for r in on})]
    groups.sort(key=lambda g: (-_mean_recall(g[1]), g[0]))
    return _summary_table(
        groups,
        caption="Per fixture — self-correction on only, strategies and both "
        "context modes pooled. Ordered by mean recall. " + _SUMMARY_LEGEND,
    )


def failure_accounting_table(records: list[RunRecord]) -> str:
    """Per-strategy recall and f1 reported both ways: including and excluding
    failed runs.

    A run that materialized nothing produced nothing evaluable: its recall is 0
    and its f1 is null (precision is undefined with no produced rows). group_mean
    drops those nulls (evaluated-only view), so the evaluated-only f1 silently
    excludes every failed run and overstates quality; group_mean_zero counts them
    as 0 (including-failed view). Showing both alongside the failed-run count
    exposes the size of that inflation.
    """
    cell_key = lambda r: r.cell  # noqa: E731
    recall_eval = group_mean(records, cell_key, lambda r: r.recall)
    recall_all = group_mean_zero(records, cell_key, lambda r: r.recall)
    f1_eval = group_mean(records, cell_key, lambda r: r.f1)
    f1_all = group_mean_zero(records, cell_key, lambda r: r.f1)

    runs = Counter(r.cell for r in records)
    failed = Counter(r.cell for r in records if not r.materialized)

    cells = sorted(runs, key=lambda c: (-f1_all.get(c, 0.0), c))

    header = [
        "Strategy", "Runs", "Failed (nothing produced)",
        "Recall incl. failed", "Recall evaluated",
        "f1 incl. failed", "f1 evaluated",
    ]
    rows = [
        _row([
            _esc(c), _esc(runs[c]), _esc(failed[c]),
            _fmt_rate3(recall_all.get(c)), _fmt_rate3(recall_eval.get(c)),
            _fmt_rate3(f1_all.get(c)), _fmt_rate3(f1_eval.get(c)),
        ])
        for c in cells
    ]
    total_failed = sum(failed.values())
    caption = (
        f"{total_failed} of {sum(runs.values())} runs materialized nothing; "
        f"'evaluated' columns drop them (inflating the mean), 'incl. failed' "
        f"count them as recall/f1 0"
    )
    return _table(header, rows, caption=caption)


def silent_failure_table(records: list[RunRecord]) -> str:
    silent = sorted(
        (r for r in records if r.silent_fail),
        key=lambda r: (r.campaign, r.model, r.cell, r.fixture, r.rep),
    )
    n = len(silent)
    caption = f"{n} silent failure" + ("" if n == 1 else "s")

    header = ["Campaign", "Model", "Cell", "Fixture", "Rep"]
    rows = [
        _row([_esc(r.campaign), _esc(r.model), _esc(r.cell), _esc(r.fixture), _esc(r.rep)])
        for r in silent
    ]
    return _table(header, rows, caption=caption)


def appendix_table(records: list[RunRecord]) -> str:
    key = _appendix_key

    recall_values = rep_values(records, key, lambda r: r.recall)
    field_acc_values = rep_values(records, key, lambda r: r.field_acc)
    cost_values = rep_values(records, key, lambda r: r.cost)
    secs_values = rep_values(records, key, lambda r: r.secs)
    tables_declared_values = rep_values(records, key, lambda r: r.tables_declared)
    cols_covered_values = rep_values(records, key, lambda r: r.cols_covered)
    mat_by_config = materialization_rate(records, key)

    configs = sorted({key(r) for r in records}, key=_norm)

    header = [
        "Model", "Cell", "Context", "Self-correct",
        "Recall (mean±sd)", "Materialization%", "Field acc (mean±sd)",
        "Cost $", "Secs", "Tables declared", "Cols covered",
    ]
    rows = []
    for model, cell, ctx, sc in configs:
        cfg_key = (model, cell, ctx, sc)
        recall_str = _fmt_mean_sd(recall_values.get(cfg_key, []))
        field_acc_str = _fmt_mean_sd(field_acc_values.get(cfg_key, []))
        mat_str = _fmt_pct(mat_by_config.get(cfg_key, 0.0))
        cost_vals = cost_values.get(cfg_key, [])
        cost_str = f"${statistics.mean(cost_vals):.4f}" if cost_vals else "-"
        secs_vals = secs_values.get(cfg_key, [])
        secs_str = f"{statistics.mean(secs_vals):.1f}" if secs_vals else "-"
        tdecl_vals = tables_declared_values.get(cfg_key, [])
        tdecl_str = f"{statistics.mean(tdecl_vals):.2f}" if tdecl_vals else "-"
        cols_vals = cols_covered_values.get(cfg_key, [])
        cols_str = f"{statistics.mean(cols_vals):.3f}" if cols_vals else "-"

        rows.append(_row([
            _esc(model), _esc(cell), _esc(ctx), _fmt_sc(sc),
            recall_str, mat_str, field_acc_str,
            cost_str, secs_str, tdecl_str, cols_str,
        ]))

    return _table(header, rows)

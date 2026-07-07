"""HTML report tables: best-config, silent-failure, and per-config appendix."""
from __future__ import annotations

import html
import statistics

from aidmi_orchestrator.report.aggregate import group_mean, materialization_rate, rep_values
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
        key = max(recall_by_config, key=recall_by_config.get)
        cell, ctx, sc, model = key
        rows.append(_row([
            "Highest mean recall", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            _fmt_rate3(recall_by_config[key]),
        ]))

    if cost_by_config:
        key = min(cost_by_config, key=cost_by_config.get)
        cell, ctx, sc, model = key
        rows.append(_row([
            "Lowest mean cost", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            f"${cost_by_config[key]:.4f}",
        ]))

    if mat_by_config:
        key = max(mat_by_config, key=mat_by_config.get)
        cell, ctx, sc, model = key
        rows.append(_row([
            "Highest materialization pass-rate", _esc(cell), _esc(ctx), _fmt_sc(sc), _esc(model),
            _fmt_pct(mat_by_config[key]),
        ]))

    return _table(header, rows)


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

    configs = sorted({key(r) for r in records})

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

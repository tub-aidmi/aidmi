from __future__ import annotations
import csv, json
from dataclasses import dataclass, asdict
from pathlib import Path

MODEL_LABELS = {
    "gemini-2.5-flash": "gemini25flash",
    "ise-ollama/qwen3.6:35b-a3b": "qwen35b",
    "nvidia/mistral-medium-3.5-128b": "mistral128b",
}

@dataclass(frozen=True)
class RunRecord:
    campaign: str; model: str; fixture: str; cell: str
    ctx: str | None; sc: bool | None; rep: int
    dbt_success: bool
    materialized: bool
    tables_materialized: float | None
    recall: float | None; precision: float | None; field_acc: float | None
    f1: float | None; recall_strict: float | None
    cost: float | None; secs: float | None
    tokens_in: int | None; tokens_out: int | None
    status: str | None; silent_fail: bool
    tables_declared: int; cols_covered: float | None
    tokens_thoughts: int | None = None
    retries: int | None = None
    cache_hit_rate: float | None = None

def _self_correction(cfg: dict) -> bool:
    """The enable_self_correction toggle, normalized to a bool.

    A strategy that omits the flag has no on/off toggle because its critique is a
    built-in structural stage (e.g. plan_write_critique); those runs count as
    self-correction on, so tables and lever plots treat them exactly like on.
    """
    value = cfg.get("enable_self_correction")
    return True if value is None else bool(value)


def _model_name(cfg: dict) -> str:
    for key in ("writer_model", "planner_model", "critic_model"):
        m = cfg.get(key)
        if isinstance(m, dict) and m.get("model_name"):
            name = m["model_name"]
            return MODEL_LABELS.get(name, name)
    return "unknown"

def _cell(row: dict, cfg: dict) -> str:
    name = row["strategy_name"]
    if name == "write_tools_freeform" and cfg.get("inline_run_dbt_tool"):
        return "write_tools_freeform_inlinedbt"
    return name

def _record(row: dict, fallback_campaign: str) -> RunRecord:
    cfg = row.get("strategy_config") or {}
    m = row.get("metrics") or {}
    per_table = m.get("gt_per_table") or {}
    dbt_success = bool(m.get("dbt_success"))
    status = m.get("strategy_status")
    tables_mat = m.get("gt_tables_materialized")
    materialized = bool(tables_mat and tables_mat > 0)
    silent = status == "complete" and (
        m.get("gt_recall_overall") is None or not tables_mat
    )
    prov = row.get("provenance") or {}
    return RunRecord(
        campaign=prov.get("campaign_id") or fallback_campaign,
        model=_model_name(cfg),
        fixture=row["fixture_name"], cell=_cell(row, cfg),
        ctx=cfg.get("context_mode"), sc=_self_correction(cfg),
        rep=row.get("rep_index", 0),
        dbt_success=dbt_success, materialized=materialized,
        tables_materialized=tables_mat,
        recall=m.get("gt_recall_overall"), precision=m.get("gt_precision_overall"),
        field_acc=m.get("gt_field_accuracy_overall"), f1=m.get("gt_f1_overall"),
        recall_strict=m.get("gt_recall_strict"),
        cost=m.get("dollar_cost_total"), secs=row.get("wall_clock_seconds"),
        tokens_in=m.get("tokens_input_total"), tokens_out=m.get("tokens_output_total"),
        status=status, silent_fail=silent,
        tables_declared=len(per_table), cols_covered=m.get("target_columns_covered"),
        tokens_thoughts=m.get("tokens_thoughts_total"),
        retries=m.get("llm_retries_total"),
        cache_hit_rate=m.get("cache_hit_rate"),
    )

def _iter_paths(paths):
    for p in paths:
        p = Path(p)
        f = p / "results.jsonl" if p.is_dir() else p
        yield f, (p.name if p.is_dir() else p.parent.name)

def load_records(paths) -> list[RunRecord]:
    out = []
    for f, fallback in _iter_paths(paths):
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(_record(json.loads(line), fallback))
    return out

def write_tidy_csv(records, path) -> None:
    path = Path(path)
    if not records:
        path.write_text(""); return
    fields = list(asdict(records[0]).keys())
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in records:
            w.writerow(asdict(r))

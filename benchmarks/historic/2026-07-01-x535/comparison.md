# Validation vs baseline

Compare this campaign against baseline [`2026-07-01-z848`](../2026-07-01-z848).

## Changes under test

- SQL sanitization at write time (strip wrappers, garbage prefixes)
- `clear_out_schema()` before each dbt run (no stale partial tables)
- Freeform model discovery fallback + nested path rejection
- `inline_run_dbt_tool: true` for write_tools_freeform
- Ground-truth recall includes missing tables in denominator
- New metric `gt_tables_materialized`
- Legacy ID / AccountId mapping guidelines

## Success criteria

| Metric | Baseline (2026-07-01-z848) | Target |
|---|---|---|
| `dbt_success` | 0/4 | ≥ 2/6 |
| freeform status | gave_up (0 models counted) | 5 tables, not gave_up |
| `gt_recall_overall` with missing Account | 1.0 (misleading) | < 1.0 when tables missing |
| `gt_tables_materialized` | n/a | present in results |

## Baseline summary

See [2026-07-01-z848/report/summary.csv](../2026-07-01-z848/report/summary.csv).

## Validation results (2026-07-01-x535)

| Fixture | Strategy | dbt_success | gt_recall | gt_tables_materialized | Notes |
|---|---|---|---|---|---|
| wrong_field_names_v2 | write_tools_freeform | **1** | 1.0 | 1.0 | 5 models, complete |
| wrong_field_names_v2 | plan_write_critique | **1** | 1.0 | 1.0 | 5 models, complete |
| wrong_field_names_v2 | write_then_critique | 0 | — | — | run failed early |
| missing_relations_v2 | write_tools_freeform | 0 | — | — | gave_up (~22s) |
| missing_relations_v2 | write_then_critique | 0 | 0.0 | 0.0 | partial, no materialized tables |
| missing_relations_v2 | plan_write_critique | 0 | 0.70 | 0.6 | partial dbt, 3/5 tables |

**dbt_success: 2/6** (meets ≥ 2/6 target). Baseline was **0/4**.

Improvements confirmed on `wrong_field_names_v2`: both freeform and plan_write_critique achieve full dbt success. Freeform no longer gives up with 0 models (baseline had empty `target_tables_written`).

`missing_relations_v2` remains hard for all strategies; plan_write_critique shows partial progress (gt_recall 0.70, 60% tables materialized) with honest metrics (no inflated 1.0 recall on missing tables).

## Validation summary

See [report/summary.csv](report/summary.csv).

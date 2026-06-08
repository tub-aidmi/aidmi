# Benchmark Campaign — Findings

> Orchestrator-expansion campaign, run 2026-06-07/08 against the TU Berlin ISE LiteLLM proxy. Aggregated report: `benchmarks/report/` (`summary.md`, `cells.csv`, `summary.csv`). Raw rows: `benchmarks/results/{main,side_context,side_selfcorr}/results.jsonl`.

## Setup

- **Strategies (6 LLM + mock):** `structured_per_table` (±self-correction), `write_tools_freeform` (+self-correction), `write_then_critique`, `plan_then_execute`, `ensemble_vote`.
- **Models (ladder):** `ise-openai-nvidia/qwen35-9b` (small), `ise-ollama/qwen3.6:35b-a3b` (mid, reasoning), `academic/devstral-2-123b-instruct-2512` (large; replaced `gpt-oss:120b` — see below), `academic/qwen3.5-397b-a17b` (anchor).
- **Fixtures:** `sp1_users` (single-table contacts→users, 8 rows) and `sf_pipedrive_snapshot` (Salesforce Contact+Account → Pipedrive-shaped persons/organizations, 200+13 rows, 11-column target).
- **Main grid:** 6 strategies × 4 models × 2 fixtures × 3 reps + mock control = 147 runs. Side studies: context modes (18) and self-correction (12), both on qwen36 × snapshot.
- **Scheduling:** model-major — `ise-*` models run one-at-a-time (exclusive groups), academic models alongside. Main pass at concurrency 3; error backfill at concurrency 1.
- **Coverage after backfill:** 141/147 main runs ok; 48/49 cells have ≥1 rep. Total corpus 177 runs.

## Headline result — `target_columns_covered` (mean), sf_pipedrive_snapshot

| strategy | devstral-123b | qwen3.5-397b | qwen3.6-35b | qwen35-9b |
|---|---|---|---|---|
| write_then_critique | 1.00 | 1.00 | 1.00 | 0.00 |
| write_tools_freeform | 1.00 | 1.00 | 0.92 | 0.67 |
| structured_per_table | 1.00 | 0.90 | 0.55 | 0.00 |
| plan_then_execute | 1.00 | 1.00 | 0.19 | 0.14 |
| ensemble_vote | 0.67 | 1.00 | 0.67 | (n/a) |

(`sp1_users` is near-saturated — every capable model/strategy reaches 1.0; it serves as the simple control and the only fixture with `row_equality` ground truth.)

## Findings

1. **Model scale dominates on the realistic fixture.** The two large models (devstral-123b, qwen3.5-397b) reach full column coverage across nearly every strategy on sf_pipedrive_snapshot. The mid qwen3.6-35b is strategy-sensitive (1.0 with critique, 0.92 freeform, but 0.19–0.55 on plan/structured). The 9B largely fails the multi-table CRM mapping regardless of strategy.

2. **Strategy choice matters most at the mid tier.** For qwen3.6-35b on the snapshot fixture, `write_then_critique` (1.0) and `write_tools_freeform` (0.92) substantially outperform `structured_per_table` (0.55) and `plan_then_execute` (0.19). The global-context strategies (critique reviews the whole proposal; freeform sees everything) beat per-table isolation when the model is capable but not frontier-class.

3. **Self-correction helps `structured_per_table`, not `write_tools_freeform`.** Side study (qwen36 × snapshot): structured `dbt_success` 0.0 → 0.33 and coverage 0 → 0.67 with self-correction on; freeform was already at 1.0 and self-correction left it unchanged (it has its own in-agent dbt loop). The structured archetype is the one that benefits from the dbt-feedback retry.

4. **Context mode (qwen36 × snapshot, side study):** `metadata_plus_samples` and `live_query_tool` both reach dbt_success 1.0 for freeform; `metadata_only` dips to 0.67 — sample rows or live querying measurably help on the realistic schema. Structured was unable to be measured cleanly here (see residual errors).

5. **Manifest quality is a hard archetype divide.** `write_tools_freeform` produces NO manifest (`manifest_present=0`) — it writes SQL files directly with no structured explanation artifact. All four structured-family strategies produce full manifests (table/column coverage 1.0). For the SP3 HITL review UI, this is decisive: freeform's strong dbt results come with nothing for a human to review.

6. **`data_preservation` surfaces real lossiness.** On the snapshot fixture, mapped runs show `preservation_distinct_ratio_mean` ≈ 0.78 (qwen36/devstral) vs ≈ 0.98 (qwen397) — the anchor model preserves source cardinality better. `row_ratio` ≈ 0.06 (13 organizations / 213 source rows) reflects the Account→organizations dedup, not loss.

## Infrastructure findings (failure-mode taxonomy)

The campaign doubled as a stress test of the ISE proxy and the strategies' robustness. Five distinct failure modes, all contained by the harness's per-run error isolation:

| Mode | Cause | Affected | Resolution |
|---|---|---|---|
| Empty tool-call response | `ise-ollama/gpt-oss:120b` returns no tool_calls via the Ollama route | All gpt-oss runs | Swapped to `devstral-2-123b` (verified tool-calls cleanly) |
| Directory write crash | freeform `write_file` crashed (`IsADirectoryError`) when a model wrote to `models/` | freeform × {devstral, qwen9b} | **Fixed** — tools return error strings, not crashes (`5079317`) |
| Rate limit (429) | academic endpoint throttles high-fan-out strategies under concurrency | ensemble/plan × academic | Backfilled at concurrency 1 — fully recovered |
| Proxy 500 | transient connection drops on long (>1000s) qwen36 snapshot runs | qwen36 × snapshot | Backfilled — recovered |
| Context window | 9B can't hold the ensemble judge prompt (all candidates × 11-col schema) | ensemble × qwen9b × snapshot | **Unrecoverable** — genuine capability limit; left as the 1 empty cell |

## Known follow-ups

- **Uncaught `UsageLimitExceeded`:** when a model loops on the freeform tool protocol and exhausts `max_tool_turns`, `write_tools_freeform.generate` crashes instead of harvesting produced files → `partial`. Affects only cells that already have surviving reps (freeform_sc × {devstral, qwen9b}). A harness-robustness fix for SP3, not campaign-blocking.
- **`ensemble_vote` on small-context models** needs a reduced judge prompt (e.g. diff-only or truncated candidates) to be viable below ~30B.
- `datetime.utcnow()` deprecation cleanup remains pending across the orchestrator.

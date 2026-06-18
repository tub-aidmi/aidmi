# Benchmark Campaign — Findings

> Orchestrator-expansion campaign, run 2026-06-07/08 against the TU Berlin ISE LiteLLM proxy. Aggregated report: `benchmarks/08-06-2026/report/` (`summary.md`, `cells.csv`, `summary.csv`, `plots/`). Raw rows: `benchmarks/08-06-2026/results/main/results.jsonl`.

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

## Findings about the approach & infrastructure

The campaign doubled as a stress test of the whole orchestration approach against a real, heterogeneous set of model endpoints. It surfaced five distinct failure modes. Some were bugs in our code; **several are findings about the approach and the model landscape that are worth carrying into SP3, not merely defects we patched.** They are separated below from the routine fixes.

### A. Tool-calling capability is model- and route-specific, and must be *qualified*, not assumed

`ise-ollama/gpt-oss:120b` returned empty completions with **no tool calls** (`finish_reason=stop`, empty content) whenever it was handed a tools array, while answering plain prompts fine. Every PydanticAI strategy depends on structured output / tool calls, so this model was unusable for *every* strategy despite being "available" on the proxy. `academic/devstral-2-123b` on a different route tool-called cleanly.

**Finding:** a model being served by the gateway is *not* sufficient — agentic strategies require verified structured-output/tool-calling on that specific model+route. The platform should gate models with a capability probe before they enter a benchmark or production rotation. (Diagnosed in ~5 minutes here; in production this is an opaque "all jobs fail" incident.)

### B. The freeform/agentic archetype is capability-gated and has a long failure tail

Two linked discoveries, in sequence:
1. Some models call `write_file("models", …)` — a directory path. The tool's unguarded `write_text` crashed the whole strategy (`IsADirectoryError`). *This one was a genuine bug in our tool* (fixed — see below).
2. Once the tool was hardened to return a recoverable error string, weaker models (devstral intermittently, qwen9b often) **looped**: they re-issued the same malformed call, consumed the error, and retried until they exhausted the 30-turn budget — a non-convergent loop, not a crash.

**Finding:** freeform's open action surface is exactly what makes it strong on capable models (qwen397/devstral reach 1.0 coverage) and fragile on weak ones (the 9B never recovers from its own mistakes). Freeform is **not a safe default for an arbitrary model** — its quality is steeply capability-gated, and unlike the structured archetypes it can fail by looping rather than by producing a wrong-but-complete answer. This is a strategy-selection finding, and it motivates two harness invariants: every tool returns errors as feedback (done), and turn-budget exhaustion must degrade to `partial` rather than crash (open — see follow-ups).

### C. Strategy sophistication trades against infrastructure friendliness

The two highest-fan-out strategies — `ensemble_vote` (N candidates × tables) and `plan_then_execute` (per-table writer fan-out) — were *precisely* the ones that tripped the academic endpoint's 429 rate limit, and only under concurrency. Simpler strategies and the local `ise-*` models were unaffected. Serializing to concurrency 1 recovered every 429 cell.

**Finding:** the most sophisticated strategies (which top the quality table) concentrate the most load. The model-exclusivity scheduling we built handles the local one-model-at-a-time constraint, but the 429s reveal the *next* required scheduling dimension: **rate-limit-aware scheduling** (per-endpoint concurrency caps + backoff). Strategy choice has an infrastructure cost, not just a token cost.

### D. Reasoning models on realistic inputs run long enough to demand orchestrator-level resilience

qwen3.6 (a reasoning model) on the 213-row snapshot produced runs of **15–50 minutes**; the longest stressed proxy connection stability and drew transient 500s.

**Finding:** per-run wall-clock on realistic data is large and highly variable, so the orchestrator's resume + per-run error containment is **load-bearing, not a nicety**. A single 12-hour campaign survived 5 failure modes and 32 individual run failures and still produced 48/49 cells, because no single failure was allowed to abort the sweep.

### Meta-finding: the benchmark platform paid for itself in one run

Each of the five failure modes above (model incompatibility, tool-boundary crash, non-convergent loop, rate-limit, context overflow) would have been a production incident in the live HITL system. The harness contained every one per-run, characterized all five, recovered the recoverable ones via a targeted concurrency-1 backfill, and left exactly one *documented* capability limit (ensemble × 9B context). **This is the argument for building the test platform: it de-risks the orchestrator against the messy reality of heterogeneous model endpoints before that orchestrator ever touches a customer's CRM.**

## Failure-mode taxonomy (summary)

| Mode | Classification | Affected | Resolution |
|---|---|---|---|
| Empty tool-call response (gpt-oss via Ollama route) | **Finding A** (model qualification) | all gpt-oss runs | Swapped to `devstral-2-123b` |
| `write_file` crash on directory path | **Bug** (our tool) → exposed **Finding B** | freeform × {devstral, qwen9b} | Fixed `5079317`: tools return error strings, not crashes |
| Freeform tool-loop / turn-budget exhaustion | **Finding B** (archetype fragility) | freeform × {devstral, qwen9b} | Surviving reps recovered; graceful-degrade fix is an open follow-up |
| Rate limit (429) under fan-out | **Finding C** (sophistication vs infra) | ensemble/plan × academic | Backfilled at concurrency 1 — fully recovered |
| Proxy 500 on long runs | **Finding D** (long-request resilience) | qwen36 × snapshot | Backfilled — recovered |
| Context window overflow | **Capability limit** (documented) | ensemble × qwen9b × snapshot | Unrecoverable — left as the 1 empty cell |

## Fixes applied during the campaign (honest record)

- **`gpt-oss:120b` → `devstral-2-123b`** swap in `main_grid.yaml` after confirming the tool-calling gap (Finding A).
- **`write_file`/`read_file` hardened** (`5079317`): directory paths and path-escapes return an error string to the agent instead of raising — a real bug fix that then exposed Finding B's loop behavior.
- **Concurrency-1 backfill** of the 29 recoverable error rows: stripped the transient/rate-limited/now-fixed error rows from `results.jsonl` and re-ran with `--resume`, recovering all but the one context-limit cell.

(Routine per-task code-review polish — bounding critique rounds, non-fatal correction crashes, deep-copying model refs, validate-before-destructive-sweep, stderr warnings — is in the branch commit history and is not reproduced here.)

## Open follow-ups

- **Graceful turn-budget handling (Finding B):** `write_tools_freeform.generate` should catch `UsageLimitExceeded`, harvest whatever models were written, and report `partial` instead of crashing. Affects only cells that already have surviving reps, so it was deferred from this campaign — but it is the concrete code change that closes Finding B.
- **Rate-limit-aware scheduling (Finding C):** add per-endpoint concurrency caps + backoff to `scheduler.py`, so high-fan-out strategies can run at speed against throttled endpoints without a manual concurrency-1 fallback.
- **Model capability probe (Finding A):** a pre-flight check that a model+route can emit a tool call / structured output, run before a model enters a sweep or production rotation.
- **`ensemble_vote` on small-context models:** needs a reduced judge prompt (diff-only or truncated candidates) to be viable below ~30B.
- **Cost reporting:** dollar cost is $0 across the board — the ISE proxy has no LiteLLM pricing entries; cost comparison needs a pricing-override JSON.
- **Methodological caveat for the numbers:** `self_reported_status` is *not* comparable across strategies (plan/ensemble never self-validate → always `complete`); use `dbt_success` and `target_columns_covered` for cross-strategy comparison.
- `datetime.utcnow()` deprecation cleanup remains pending across the orchestrator.

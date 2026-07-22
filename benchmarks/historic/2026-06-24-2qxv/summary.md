# Benchmark summary — `2026-06-24-2qxv`

**Label:** google cloud master  
**Fixture:** `master` — German `master_*` source tables → Salesforce-style target (Account, Contact, Opportunity, Project__c, Installed_Asset__c)  
**Runs:** 1 per strategy cell (`runs_per_cell: 1`)  
**Writer model:** `gemini-2.5-flash` (Google Cloud, thinking budget 2048)  
**Planner / critic model (plan_write_critique only):** `gemini-2.5-pro`

| Run ID | Strategy | Final dbt | Wall clock | Cost |
|---|---|---|---|---|
| `r83qekgvr_write_tools_freeform_master` | `write_tools_freeform` | **pass** (5/5) | 2m 43s | $0.063 |
| `r5fvhfdge_plan_write_critique_master` | `plan_write_critique` | **fail** (0/5 on final run) | 7m 01s | $0.421 |

---

## Executive summary

On this campaign, **`write_tools_freeform` is the clear winner**: it finished in ~2.5 minutes at ~$0.06, ran dbt successfully end-to-end, and materialized all five target tables in Postgres with correct row counts.

**`plan_write_critique` spent ~7× the time and ~6.7× the money**, produced richer mapping documentation, and briefly achieved a full successful dbt run during its initial correction loop — but **regressed during critique-driven revisions** and **ended with a failed final dbt run**. The harness still reports `target_columns_covered = 1.0` because Postgres retains tables from the earlier successful materialization; that metric must not be read as “final SQL is deployable.”

The decisive failure mode for plan_write_critique was **`Opportunity.Amount` parsing of European-formatted currency strings** (e.g. source `316.863,04` → invalid cast `"316.863.04"`). The freeform strategy solved the same problem with a simpler approach: strip non-numeric characters, remove thousand-separator dots, then swap comma to decimal point.

---

## Fixture and source data

The `master` fixture loads five German-named source tables into schema `fixture_master_src`:

| Source table | Rows | Target table |
|---|---:|---|
| `master_kunden` | 210 | Account |
| `master_kontakte` | 497 | Contact |
| `master_opportunities` | 625 | Opportunity |
| `master_projekte` | 342 | Project__c |
| `master_assets` | 900 | Installed_Asset__c |

Source data is intentionally messy: mixed German/English enum labels, European number formats (`316.863,04`), multiple date string formats, sentinel dates (`0000-00-00`, `N/A`), currency tokens (`EUR`, `€`, `Dollar`), and inconsistent casing (`SILBER`, `Silber`, `silver`, `GOLD`, `Platin`, etc.).

The target schema defines **57 columns** across five tables (51 text, 5 integer, 1 double precision on `Opportunity.Amount`).

---

## Evaluated metrics (harness)

Both strategies wrote all five models and introspected Postgres as having the full target column set with matching types (`target_columns_covered = 1.0`, `type_mismatches = 0`, `extraneous_columns = 0`). **`dbt_success` is where they diverge.**

| Metric | write_tools_freeform | plan_write_critique |
|---|---:|---:|
| `dbt_success` | 1 | **0** |
| `dbt_models_succeeded` / `failed` | 5 / 0 | 0 / 5 (final run) |
| `ran_ok` | 1 | 1 |
| `wall_clock_seconds` | 163 | 421 |
| `llm_calls_total` | 28 | 41 |
| `tokens_input_total` | 177k | 224k |
| `tokens_output_total` | 19k | 105k |
| `dollar_cost_total` | $0.063 | $0.421 |
| `cache_hit_rate` | 78% | 48% |

**Interpretation:** plan_write_critique’s higher token output (especially writer: 92k vs 19k) reflects per-table structured rewrites, critique feedback loops, and verbose `column_notes` / reasoning in the manifest. The lower cache hit rate is consistent with repeatedly changing prompts as dbt errors and critic comments arrive.

There is **no row-equality evaluator** on this fixture (no reference dbt project), so the harness does not score semantic correctness of cell values — only schema shape and dbt execution status.

---

## Postgres output (queried live)

Schemas inspected: `r83qekgvr_write_tools_freeform_master` and `r5fvhfdge_plan_write_critique_master`.

### Row counts

Both schemas contain all five tables. Counts match source tables 1:1:

| Table | Source rows | Freeform | Plan/write/critique |
|---|---:|---:|---:|
| Account | 210 | 210 | 210 |
| Contact | 497 | 497 | 497 |
| Opportunity | 625 | 625 | 625 |
| Project__c | 342 | 342 | 342 |
| Installed_Asset__c | 900 | 900 | 900 |

**Important caveat for plan_write_critique:** these rows come from an **intermediate successful dbt run** (~12:33 UTC). Later failed runs did not refresh the tables (dbt `fail_fast` skips downstream models; failed models leave prior materializations in place). The SQL on disk at the end of the run **does not successfully execute** against the full dataset even though Postgres still looks complete.

### Data quality comparison (selected checks)

| Check | write_tools_freeform | plan_write_critique |
|---|---|---|
| Contact `AccountId` NULL | 0 | 62 |
| Contact orphan `AccountId` (not in Account) | **62** | 0 |
| Project__c orphan `Account__c` | **39** | 0 |
| Opportunity `Amount` NULL | 0 | 114 |
| Opportunity `Amount` = 0 | 214 | 100 |
| `StageName` distribution | identical top-5 buckets | identical (stale good run) |
| Contact `Role__c` = Decision Maker | **147** (maps German `ENTSCHEIDER`) | 72 (English labels only) |
| Contact `Role__c` NULL | 222 | 297 |
| Account `Customer_Tier__c` NULL | 37 | 52 |
| Account Silver / Platinum mapped | 27 / 65 | 37 / 40 |

**Takeaways:**

- **Freeform** preserves every contact→account reference but **does not validate FK existence** — 62 contacts point at customer IDs that are not in Account (direct `kd_nummer` copy without join/filter).
- **Plan/write/critique** uses **LEFT JOINs to `master_kunden`** for Contact, Project, and Asset models, yielding **valid FKs or NULL** — better relational hygiene on the materialized snapshot.
- **Freeform** maps more German source role values (`ENTSCHEIDER` → Decision Maker); plan/write/critique’s English-only CASE leaves more NULLs.
- **Freeform** coerces unparseable amounts to **0** (`COALESCE(..., '0')`), which avoids cast errors but **inflates zero amounts** (214 rows). The plan/write/critique snapshot (from the earlier good run) uses NULL for unparseable amounts (114 NULLs) — semantically safer.
- **Customer tier:** freeform maps `PLATIN` → Platinum; plan/write/critique only maps exact `platinum`, leaving more NULLs but fewer questionable Platinum assignments.

---

## dbt SQL — what each strategy produced

### write_tools_freeform (`r83qekgvr`)

**Shape:** flat `SELECT` per table, no CTEs, minimal joins. Uses `query_postgres` heavily during authoring to inspect DISTINCT enum values before writing CASE expressions.

**Representative patterns:**

- **Account:** direct column maps; `COALESCE` for NOT NULL `Name`; German tier synonyms via `UPPER` (`SILBER` → Silver, `PLATIN`/`PLATINUM` → Platinum).
- **Contact:** maps `ENTSCHEIDER` and other German roles; copies `kd_nummer` straight to `AccountId` (no join).
- **Opportunity — Amount (the hard column):**

```sql
COALESCE(
  NULLIF(
    REPLACE(
      REPLACE(
        REGEXP_REPLACE(TRIM(COALESCE(auftragswert, '')), '[^0-9,.\-]+', '', 'g'),
      '.', ''),
    ',', '.'),
  ''),
'0')::DOUBLE PRECISION AS "Amount"
```

  Strips currency text, removes `.` thousand separators, converts `,` decimal separator, defaults empty to `0`. This correctly parses `316.863,04` → `316863.04`.

- **Project__c / Installed_Asset__c:** direct FK copies from source keys (no join validation).

**dbt journey (4 runs):**

1. Fail — `Opportunity`: cast empty string to double precision  
2. Fail — invalid regex escape in date patterns  
3. Fail — empty string cast again  
4. **Success** — all 5 models  
5. Final harness dbt pass — success  

Total self-correction: ~4 dbt cycles in ~2.5 minutes.

### plan_write_critique (`r5fvhfdge`)

**Shape:** richer SQL — CTEs, explicit JOINs to resolve foreign keys, `INITCAP`/`TO_CHAR` formatting, detailed manifest with per-column mapping notes from planner + writer.

**Representative patterns:**

- **Account:** `INITCAP` on names/cities; `LOWER`-based tier mapping; timestamps via `TO_CHAR(CURRENT_TIMESTAMP, ...)`.
- **Contact:** JOIN `master_kunden` on `kd_nummer = kundennummer`; `COALESCE(..., 'N/A')` on LastName; critic later requested non-NULL timestamps.
- **Opportunity — Amount (final on-disk SQL, still fails at runtime):**

  Multi-branch CASE distinguishing European vs US separators. The intended fix for `316.863,04` is the branch “dot before comma → strip dots, swap comma.” **During the run, an intermediate version hit the “comma only” branch** and produced `"316.863.04"`, causing:

  ```
  invalid input syntax for type double precision: "316.863.04"
  ```

  (Re-running the final on-disk expression against all 625 source rows in Postgres succeeds today — the failure was tied to a revision mid-critique loop, not necessarily the last saved file state vs what dbt compiled.)

- **Project__c / Installed_Asset__c:** JOINs to kunden/opportunities for validated FKs; more defensive date parsing.

**dbt journey (7 runs):**

| Phase | Time (UTC) | Result |
|---|---|---|
| Initial materialization | 12:32:36 | Fail — `Opportunity`: `0000-00-00` date out of range; 4 models succeed |
| Initial correction loop | 12:33:29 | **All 5 succeed** |
| Critique round 1 | 12:34:41 | Critic approves 4/5; **Contact needs revision** (NULL timestamps) |
| Post-critique dbt | 12:34:59 | Fail — Contact SQL contains garbage token `情緒` (markdown leakage) |
| Correction | 12:36:23 | Fail — Opportunity amount + Contact `normalize_text` pseudo-function |
| Final attempts | 12:37:42–49 | Fail — Opportunity amount `"316.863.04"`; fail_fast skips rest |

**Critique loop assessment:** The critic correctly spotted real issues (NULL audit timestamps on Contact) but **triggered revisions that introduced worse defects** — markdown/CJK characters in SQL, pseudo-function names, and fragile amount logic. The strategy exhausted budget fixing self-inflicted regressions without returning to the known-good Opportunity model from the initial correction pass.

---

## What went well

### write_tools_freeform

- **End-to-end success** on first campaign run with modest cost and latency.
- **Pragmatic exploration:** ~21 `query_postgres` calls to sample DISTINCT values before encoding CASE mappings.
- **Robust European amount parsing** with a short, battle-tested pattern.
- **German source awareness** in enums (e.g. `ENTSCHEIDER`, `SILBER`, `GEWONNEN`).
- **Self-correction converged** in four dbt iterations without multi-model orchestration overhead.

### plan_write_critique

- **High-quality planning pass** (~60s planner): global FK strategy, shared date/amount normalization plan, per-table pitfall notes.
- **Structured manifest** documenting source→target column rationale (valuable for human review / downstream tooling).
- **Better join semantics** on Contact, Project, and Asset — no orphan FKs in the materialized snapshot.
- **Initial correction loop succeeded** — proving the multi-model pipeline can reach a valid state before critique.
- **Critic used live Postgres queries** on the output schema and found a legitimate data-quality issue (NULL audit fields).

---

## What did not go well

### write_tools_freeform

- **62 orphan Contact `AccountId` values** — copies foreign keys without verifying parent existence.
- **39 orphan Project `Account__c` values** — same pattern.
- **214 Opportunity amounts forced to 0** — masks parse failures; bad for analytics.
- **Hardcoded `'2023-01-01'` audit timestamps** — not load-time values.
- **Weaker normalization** (no `INITCAP`, limited language/role enum coverage compared to what plan_write_critique attempted).
- **Regex fragility during self-correction** — invalid escape sequences caused intermediate failures before settling.

### plan_write_critique

- **Final dbt run failed** — primary benchmark failure; `dbt_success = 0`.
- **Critique loop caused regressions** worse than the original issues (SQL syntax garbage, broken amount parser).
- **6.7× cost, 2.6× wall clock** for a worse execution outcome.
- **Writer churn:** 36 writer LLM calls vs 28 total calls for freeform; 92k output tokens vs 19k.
- **Stale Postgres vs final SQL** — metrics look successful while deployable artifacts are not.
- **Amount logic complexity** without empirical validation — multi-branch CASE still lost to a known fixture row (`OPP-M-00007`, `316.863,04`).
- **Only one critique round** before corrections spiraled — no recovery to last-known-good model files.

---

## Root cause: why plan_write_critique lost

1. **European currency in `auftragswert`** — hundreds of values like `231.709,49` and `316.863,04`. The writer’s “improved” parser mishandles dot+comma values when branch selection or compiled regex diverges, yielding `"316.863.04"`.

2. **Critique-induced rewrite risk** — structured output (`TableMapping.dbt_sql`) plus revision prompts led to **non-SQL tokens** (`情緒`, `` ```sql ``, `normalize_text`) in emitted files. Freeform’s tool-based `write_file` loop also risks this, but did not hit it here.

3. **fail_fast + stale tables** — partial success hides final failure in schema metrics; only `dbt_success` and trace forensics reveal the true end state.

4. **Over-engineering vs empirical testing** — freeform’s amount fix is naive but was **validated by repeated dbt runs over all 625 rows**; plan_write_critique’s elaborate CASE was not kept stable across revisions.

---

## Strategy comparison (when to use which)

| Dimension | write_tools_freeform | plan_write_critique |
|---|---|---|
| Time to first full dbt pass | ~2.5 min | ~3 min (before regressions) |
| Final deployable dbt project | **Yes** | **No** (this run) |
| Cost efficiency | **High** | Low |
| Mapping documentation | None (`manifest: null`) | **Rich manifest** |
| FK integrity | Weak (direct copies) | **Stronger joins** |
| Enum / locale coverage | **Better German mappings** (this run) | Better English mappings; more NULLs |
| NULL vs 0 for bad amounts | Zeros (arguably wrong) | NULLs in good snapshot (better) |
| Operational complexity | Single writer agent | Planner + writer + critic + 3 correction budgets |
| Failure mode | Localized self-correction | **Global regressions** across critique loop |

---

## Recommendations

1. **Treat `dbt_success` as the primary pass/fail gate** for campaigns; pair with `target_columns_covered` only when the final dbt run succeeded.

2. **Add row-level or spot-check evaluators** on `master` (even without a full reference project) — FK orphan counts, NULL rate on required enums, amount parse rate — to catch the issues visible in Postgres here.

3. **For plan_write_critique:** pin last-known-good model SQL before applying critic revisions; reject writer output that fails a SQL sanitizer (no markdown fences, no non-ASCII outside string literals).

4. **Share the freeform Amount normalization snippet** as a fixture hint or shared macro — European formats are the recurring footgun across campaigns.

5. **Re-run with `runs_per_cell ≥ 3`** — single-run cells are anecdotal, especially for stochastic critique loops.

6. **Consider disabling critique** until initial dbt passes are stable, or cap critique to metadata-only (notes) without rewriting SQL.

---

## Artifacts

| Path | Contents |
|---|---|
| `runs/r83qekgvr_write_tools_freeform_master/` | dbt project, trace, result |
| `runs/r5fvhfdge_plan_write_critique_master/` | dbt project, trace, result, manifest |
| `report/summary.csv` | Aggregated metrics |
| `report/plots/master/` | Comparison charts (dbt_success, cost, tokens, funnel) |
| Postgres schemas | `r83qekgvr_write_tools_freeform_master`, `r5fvhfdge_plan_write_critique_master` |

**Git at campaign creation:** `3782a31` on branch `add-fixtures` (dirty working tree).

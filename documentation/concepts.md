# Concepts

This page describes the units the orchestrator is built from and how they interact. Code locations refer to `packages/orchestrator/src/aidmi_orchestrator/`.

## Orchestrator

The orchestrator is a fixed sequential flow defined in `orchestrator.py:run_orchestrator`:

1. Scaffold a per-run directory and an empty dbt project.
2. Call SP1's `extract_source` to load the fixture's raw data into `src_<run-id-lower>_raw`.
3. Introspect **only** that raw schema into a `SourceSummary` (table names, column types and nullability, sample rows).
4. Construct an `OrchestratorAPI` instance and pass it to `strategy.generate(api)`.
5. Run the dbt project the strategy wrote (`api.run_dbt()`), recording the final outcome.
6. Run each registered evaluator against the post-state and persist the merged metrics.

The orchestrator does not loop. There is no orchestrator-level iteration on dbt errors. If a strategy needs iteration, it implements it internally — typically by calling `api.run_dbt()` mid-flow, reading the result, and re-editing the SQL files before returning.

## Strategy

A strategy is an opaque function from the orchestrator's API to a `StrategyResult`. It has total freedom in how it produces the dbt project: one LLM call or many, structured Pydantic output or free-form tool calls, single model or multiple. The contract is in `strategy/base.py`:

```python
class Strategy(Protocol):
    name: str
    config: BaseModel
    async def generate(self, api: OrchestratorAPI) -> StrategyResult: ...
```

The orchestrator hands the strategy an `OrchestratorAPI` containing:

- `source_summary` — pre-computed schema introspection plus sample rows (raw schema only).
- `target_schema` — the target schema if the fixture provided one, else `None`.
- `dbt_project_path` — the directory the strategy writes `.sql` files into.
- `staging_db_url` — Postgres connection string.
- `staging_raw_dataset`, `staging_out_dataset` — raw extract and dbt output schema names (`src_<id>_raw` / `src_<id>_out`).
- `trace` — a sink that auto-records every traced operation.
- `make_llm(spec, role)` — constructs a `TracedModel` wrapping a PydanticAI model.
- `run_dbt()` — executes dbt against the current state of the dbt project; auto-traced.
- `read_table_sample(schema, table, n)` and `query_postgres(sql)` — direct staging reads.

The strategy returns a `StrategyResult` describing what it wrote and an optional `MappingManifest` documenting the per-column mapping decisions in structured form.

### Built-in strategies

| Name | Paradigm |
|------|----------|
| `mock` | Reads a JSON file describing the mapping and writes it to disk. No LLM. Used in tests and as a benchmark baseline. |
| `structured_per_table` | One PydanticAI agent per target table, running in parallel via `asyncio.gather`. Each agent returns a typed `TableMapping` (full `dbt_sql` string plus column notes). Supports optional self-correction (`enable_self_correction`): re-runs dbt after each table pass and feeds errors back to the agent for up to `max_self_correction_passes` rounds. |
| `write_tools_freeform` | A single PydanticAI agent given `write_file`, `read_file`, optionally `query_postgres`, and optionally `run_dbt` as tools. Lays out the dbt project however it sees fit. Optionally self-corrects by re-running dbt. |
| `write_then_critique` | Two-agent loop: a writer produces SQL, a critic (same or different model) reviews it and returns structured feedback, the writer revises. Repeats for up to `max_critique_rounds` passes. |
| `plan_then_execute` | A planner agent produces a structured mapping plan (table-by-table column assignments); a separate writer agent (same or different model) turns the plan into dbt SQL. |
| `ensemble_vote` | Runs `n_candidates` independent write passes, then uses a judge agent to select the best candidate per target table. |

## Evaluator

An evaluator inspects the run artifacts after the orchestrator's final dbt pass and returns a dictionary of metric names to values. The Evaluator protocol (`evaluator/base.py`):

```python
class Evaluator(Protocol):
    name: str
    def applies_to(self, artifacts: RunArtifacts) -> bool: ...
    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]: ...
```

`applies_to` lets an evaluator self-skip — for example, `row_equality` only runs when the fixture provides a reference dbt project to compare against.

`RunArtifacts` includes `staging_raw_dataset` / `staging_out_dataset`. Built-in **`schema`** and **`row_equality`** evaluators compare **production** outputs in PostgreSQL's **`_out`** schema; **`row_equality`** copies **`_raw`** tables into `{raw}_reference` before running fixture reference dbt.

Metric names are free-form. Each evaluator emits whatever keys it wants; the harness merges all evaluators' output into the `BenchmarkResult.metrics` dictionary. Adding a new evaluator does not require a schema change.

### Built-in evaluators

| Name | When it runs | Sample metrics |
|------|--------------|----------------|
| `execution` | Always | `dbt_success`, `dbt_models_succeeded/failed`, `dbt_error_messages` |
| `llm_usage` | Trace contains LLM call events | `llm_calls_by_role`, `tokens_input_total`, `tokens_input_cached`, `cache_hit_rate`, `dollar_cost_total`, `dollar_cost_by_role`, `latency_ms_p95_by_role` |
| `schema` | Always | `produced_column_count`, `produced_type_histogram`; coverage metrics when a target schema is available |
| `row_equality` | Fixture has a reference dbt project | `row_count_match`, `row_set_diff_count`, per-table `column_value_match_rate` |
| `manifest_quality` | Strategy produced a `MappingManifest` | Scores the per-column mapping notes for completeness and specificity. |
| `data_preservation` | Always | Checks that non-nullable source columns appear in at least one output model; flags columns dropped without a note. |

`llm_usage` prices each call using LiteLLM's `model_cost` table. For models LiteLLM does not know about (custom Ollama tags, internal proxies), an optional `configs/pricing.json` override file maps `provider/model_name` to per-token rates.

## Fixture

A fixture is a Python sub-package under `aidmi_orchestrator.fixtures/` that registers itself at import time. Its `__init__.py` calls `register_fixture(...)` with:

- A `name` for CLI lookup.
- A `source_factory` callable that returns a `dlt` source.
- A `target_schema_path` (optional) — JSON-serialized `TargetSchema`.
- A `reference_dbt_path` (optional) — full dbt project for tier-3 row-equality scoring.
- A list of `applicable_evaluators` names.

The bundled `sp1_users` fixture provides all four: filesystem JSONL source data, a target schema describing the `users` table, the hand-written reference dbt project carried over from sub-project 1, and the full evaluator list.

A fixture's data files live alongside its `__init__.py` in the same sub-package directory. There is no dynamic file loading; everything is reachable via normal Python imports. Custom fixtures follow the same pattern in their own packages.

## Run, benchmark, sweep

- A **run** is one invocation of `run_orchestrator(fixture, strategy, run_id, workspace, staging_db_url)`. It produces a `runs/<run-id>/` directory.
- A **benchmark** wraps a run with evaluator invocation and produces a `BenchmarkResult`. The Python entry point is `Benchmark.run(strategy, strategy_spec_name="…")` (often from `parse_strategy_spec()` plus `make_strategy()` for file-based workflows).
- A **sweep** runs multiple `(strategy, config)` cells across one or more fixtures and streams the resulting `BenchmarkResult` rows to a JSONL file. The scheduler uses **model-major ordering**: models whose `model_name` starts with an exclusive prefix (default `ise-`) are serialized one at a time so that only one large model is loaded on the ISE cluster at once; all other (passthrough) models run in parallel up to the configured concurrency. Interrupted sweeps are resumable: already-written rows in `results.jsonl` are skipped on the next invocation.

## Registries

Four registries follow one pattern (`register_X`, `list_X`, `make_X`):

| Registry | Location | Registered at import of |
|----------|----------|--------------------------|
| Providers | `llm.py` | `aidmi_orchestrator.llm` |
| Strategies | `strategy/base.py` | `aidmi_orchestrator.strategy` (transitively imports each sub-package) |
| Evaluators | `evaluator/base.py` | `aidmi_orchestrator.evaluator` |
| Fixtures | `fixtures/base.py` | `aidmi_orchestrator.fixtures` |

The CLI imports the four parent packages at startup, which is enough to populate all built-in registrations. User-supplied registrations live in user code and are added by importing the user's modules before invoking the harness.

There is no dynamic file loading. Every registered entry is a regular Python module.

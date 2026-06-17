# Architecture

This page describes the internal structure of the orchestrator package, for contributors. End users do not need to read it.

## Workspace layout

```
aidmi/                                     # repository root
├── packages/
│   ├── pipeline/                          # sub-project 1: dlt + dbt substrate
│   │   └── src/aidmi_pipeline/
│   │       ├── config.py                  # MigrationRun, StagingConfig
│   │       ├── migration.py               # extract_source, transform, load_target
│   │       └── cli.py
│   └── orchestrator/                      # sub-project 2: this package
│       └── src/aidmi_orchestrator/        # see below
├── documentation/                         # this directory
└── frontend/                              # sub-project 4 (not yet implemented)
```

`packages/orchestrator/` depends on `packages/pipeline/` as a workspace package. The orchestrator calls SP1's `extract_source()` and `transform()` but never calls `load_target()` — production reverse-ETL is sub-project 3.

## Orchestrator package layout

```
packages/orchestrator/src/aidmi_orchestrator/
├── domain.py                              # Pydantic types: SourceSummary, TargetSchema, ModelSpec,
│                                          # MappingManifest, StrategyResult, BenchmarkResult
├── trace.py                               # TraceSink + TraceEvent hierarchy
├── pricing.py                             # LiteLLM model_cost lookup + JSON override
├── llm.py                                 # provider registry, make_llm, TracedModel
├── discover.py                            # Postgres introspection → SourceSummary
├── api.py                                 # OrchestratorAPI (the surface strategies receive)
├── persistence.py                         # File writers
├── orchestrator.py                        # run_orchestrator() — the 6-step flow
├── benchmark.py                           # Benchmark.run / .sweep + grid expansion
├── cli.py                                 # Typer wrapper
├── strategy/
│   ├── base.py                            # Strategy Protocol + registry + helpers
│   ├── mock/
│   ├── structured_per_table/
│   └── write_tools_freeform/
├── evaluator/
│   ├── base.py                            # Evaluator Protocol + RunArtifacts + registry
│   ├── execution.py
│   ├── llm_usage.py
│   ├── schema.py
│   └── row_equality.py
└── fixtures/
    ├── base.py                            # Fixture dataclass + registry
    └── sp1_users/
```

## Module responsibilities

Each module has one clear responsibility:

| Module | Responsibility |
|--------|----------------|
| `domain.py` | Pure Pydantic data types. No I/O, no behavior beyond validation. |
| `trace.py` | Append-only JSONL writer and the event-class hierarchy. No business logic. |
| `pricing.py` | Look up a `(provider, model_name)` price. Falls back from override file to LiteLLM table to `None`. |
| `llm.py` | Provider registry, built-in provider factories, `make_llm`, and `TracedModel`. No knowledge of strategies. |
| `discover.py` | One function: query `information_schema` and assemble a `SourceSummary`. |
| `api.py` | The `OrchestratorAPI` dataclass — the typed surface strategies use. Delegates dbt invocation to SP1's `transform()`. |
| `persistence.py` | File writers only. No business logic, no observation of state. |
| `orchestrator.py` | The sequential flow: extract → discover → strategy → final dbt → persist. The only place that wires SP1 to the orchestrator. |
| `benchmark.py` | `Benchmark.run` invokes the orchestrator + evaluators and writes a `BenchmarkResult` (requires `strategy_spec_name`). `Benchmark.sweep` loops over `(strategy, spec label)` tuples. `expand_grid` does cartesian product expansion of a grid YAML. |
| `cli.py` | Typer wrapper. YAML deserialization and CLI argument plumbing only. |
| `strategy/base.py` | Protocol, registry, and three shared helpers (`build_context_prompt`, `write_proposal`, `build_manifest_from_notes`). |
| `evaluator/base.py` | Protocol, `RunArtifacts`, `FixtureMetadata`, registry. |
| `fixtures/base.py` | `Fixture` dataclass, registry. |

## Control flow

For one orchestrator run:

```
1. CLI / benchmark.py
   │
   │ benchmark.run(strategy, strategy_spec_name=...)
   ▼
2. orchestrator.py:run_orchestrator
   │
   ├─ persistence.scaffold_dbt_project        # writes dbt_project.yml
   ├─ TraceSink(run_dir / "trace.jsonl")     # opens the trace file
   │
   ├─ aidmi_pipeline.migration.extract_source # populates Postgres staging schema
   │   └─ trace.record(StrategyEvent("extract_complete"))
   │
   ├─ discover.discover                       # introspect into SourceSummary
   │   └─ trace.record(StrategyEvent("discover_complete"))
   │
   ├─ Construct OrchestratorAPI
   │
   ├─ await strategy.generate(api)
   │   │
   │   ├─ api.make_llm(spec, role)            # returns TracedModel wrapping a PydanticAI Model
   │   ├─ agent.run(...)                      # PydanticAI; TracedModel.request records an LlmCallEvent
   │   ├─ strategy may call api.run_dbt()     # records a DbtRunEvent
   │   ├─ strategy may call api.query_postgres / api.read_table_sample
   │   └─ returns StrategyResult
   │
   ├─ try: await api.run_dbt()               # final canonical dbt run
   │   except: trace.record(StrategyEvent("final_dbt_failed"))
   │
   ├─ persistence.write_strategy_result
   ├─ persistence.write_mapping_manifest      # iff strategy_result.manifest is not None
   ├─ trace.close()
   │
   └─ return RunArtifacts
        │
        ▼
3. benchmark.py
   │
   ├─ For each registered evaluator:
   │     if ev.applies_to(artifacts): metrics.update(ev.evaluate(artifacts))
   │
   ├─ Construct BenchmarkResult(metrics=metrics, ...)
   ├─ persistence.write_benchmark_result      # writes result.json
   └─ return BenchmarkResult
```

A sweep is `benchmark.sweep` looping the above once per cell.

## Report pipeline

`aidmi-orchestrator report` aggregates `results.jsonl` without re-running evaluators:

```
results.jsonl
  → aggregate (CellAggregate + RepSeries)
  → build_report_plan (ReportContributor descriptors → headline metrics + plot recipes)
  → write_tables / write_markdown (root summary)
  → write_plots (per-recipe SVG + sibling CSV under plots/{fixture}/)
```

Report contributors live in `aidmi_orchestrator/report/contributors/` and mirror evaluator families. Each declares `MetricDescriptor`s (headline table columns, plot scopes, chart styling). Plot recipes pair a `PlotScope` (`global`, `by_strategy`, `by_model`) with a `PlotKind` (`heatmap`, `distribution`, …). v1 implements global heatmaps only.

## Registries

All four registries follow the same shape:

```python
_REGISTRY: dict[str, tuple[type, type[BaseModel] | None]] = {}

def register_X(name, cls, config_cls=None):
    if name in _REGISTRY:
        raise ValueError(f"X {name!r} already registered")
    _REGISTRY[name] = (cls, config_cls)

def list_X() -> list[str]:
    return sorted(_REGISTRY)

def make_X(name, config_dict=None):
    if name not in _REGISTRY:
        raise ValueError(f"unknown X {name!r}. Registered: {list_X()}")
    cls, config_cls = _REGISTRY[name]
    cfg = config_cls(**(config_dict or {})) if config_cls is not None else None
    return cls(cfg) if cfg is not None else cls()
```

The four parent packages (`aidmi_orchestrator.strategy`, `aidmi_orchestrator.evaluator`, `aidmi_orchestrator.fixtures`, and `aidmi_orchestrator.llm`) trigger built-in registrations at import time:

| Package | What imports trigger |
|---------|----------------------|
| `aidmi_orchestrator.strategy` | Imports `base`, `mock`, `structured_per_table`, `write_tools_freeform` sub-packages. Each sub-package's `__init__.py` calls `register_strategy`. |
| `aidmi_orchestrator.evaluator` | Imports `base`, `execution`, `llm_usage`, `schema`, `row_equality` modules. Each module top-level calls `register_evaluator`. |
| `aidmi_orchestrator.fixtures` | Imports `base` and each fixture sub-package. Each fixture's `__init__.py` calls `register_fixture`. |
| `aidmi_orchestrator.llm` | Top-level of `llm.py` calls `register_provider` for the five built-ins (`openai`, `openai_compatible`, `ollama`, `anthropic`, `litellm`). |

`cli.py` imports the four parent packages at module top-level, which is sufficient to populate all built-in registrations. User code wishing to register additional entries imports the relevant user modules before invoking the CLI or benchmark.

## TracedModel

`TracedModel` is a subclass of PydanticAI's `WrapperModel`. It overrides one method:

```python
async def request(self, messages, model_settings, model_request_parameters) -> ModelResponse:
    start = time.perf_counter()
    response = await self.wrapped.request(messages, model_settings, model_request_parameters)
    latency_ms = (time.perf_counter() - start) * 1000
    self._trace.record(LlmCallEvent(
        timestamp=datetime.utcnow(),
        role=self._role,
        model_spec=self._spec,
        messages=[m.model_dump() for m in messages],
        response=response.model_dump(),
        usage={
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "cache_read_tokens": response.usage.cache_read_tokens,
            "cache_write_tokens": response.usage.cache_write_tokens,
        },
        latency_ms=latency_ms,
    ))
    return response
```

PydanticAI normalizes provider-specific cache fields (OpenAI `cached_tokens`, Anthropic `cache_read_input_tokens`) into the `cache_read_tokens` field, so `TracedModel` does not need to branch on provider.

`api.make_llm(spec, role)` returns a `TracedModel`, not a bare PydanticAI `Model`. Strategies that bypass `api.make_llm` and construct their own models lose tracing.

## Pipeline integration

The orchestrator depends on `aidmi_pipeline` (sub-project 1) for two functions:

- `aidmi_pipeline.migration.extract_source(run: MigrationRun) -> ExtractResult` — runs dlt to load source data into Postgres. Called once at the start of the run.
- `aidmi_pipeline.migration.transform(run: MigrationRun) -> TransformResult` — runs the dbt project under `run.dbt_project_path`. Called by `api.run_dbt()` and again at the end of the run.

The orchestrator constructs a `MigrationRun` with:
- `source` — the fixture's `source_factory()` result.
- `staging` — `StagingConfig.for_run(db_url, run_id)`, i.e. `raw_dataset_name=src_<run_id_lower>_raw` and `out_dataset_name=src_<run_id_lower>_out`. Extract uses the raw schema; dlt's dbt integration sets `target.schema` to the **out** schema so models materialize there. The ULID portion is lowercased to match Postgres / dlt normalization.
- `target` and `target_dataset` — `None` and `""`, unused during generation.
- `target_tables` — empty, unused.
- `dbt_project_path` — the per-run scaffolded directory.

`load_target` is never called by SP2. Production data movement is sub-project 3's responsibility.

## Test layout

```
packages/orchestrator/tests/
├── conftest.py                      # testcontainers Postgres + Podman socket detection
├── unit/
│   ├── test_domain.py
│   ├── test_trace.py
│   ├── test_pricing.py
│   ├── test_llm_construction.py
│   ├── test_discover.py
│   ├── test_api.py
│   ├── test_registries.py
│   ├── test_evaluators.py
│   └── test_persistence.py
├── integration/
│   └── test_orchestrator_mock.py    # the success criterion: MockStrategy end-to-end
└── llm_smoke/
    └── test_real_llm.py             # @pytest.mark.requires_llm; skipped without API key
```

Unit tests cover one module each. The integration test runs `Benchmark.run` end-to-end with `MockStrategy` against the `sp1_users` fixture and asserts the metrics the spec calls out as success criteria. The LLM smoke test exercises `structured_per_table` against a real OpenAI endpoint when `OPENAI_API_KEY` is set; CI skips it.

Run the deterministic suite:

```bash
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

The integration test starts a Postgres container; expect 50–60 seconds of wall-clock.

## Error semantics

The orchestrator catches only strategy crashes. Everything else is observed, not intercepted.

| Failure | Where | Effect |
|---------|-------|--------|
| Strategy raises | step 4 of `run_orchestrator` | `StrategyExecutionError`; `BenchmarkResult.error` set; evaluators with `applies_to=True` for partial state still run; sweep continues to next cell. |
| Final dbt fails | step 5 | Caught and recorded as `final_dbt_failed` event; `final_transform_result=None`; `ExecutionEvaluator` records `dbt_success=False`. |
| LLM call fails | inside strategy | The strategy's problem. Catching and retrying is fine; if not caught, becomes a strategy crash. |
| dbt fails mid-strategy | inside strategy's `api.run_dbt()` call | The strategy's problem. Trace event is still written. |
| Postgres unreachable | benchmark or run start | Hard fail of the sweep. |
| Pricing lookup misses | `LlmUsageEvaluator` | Cost recorded as `null`; warning logged. Other metrics unaffected. |

## Adding a new orchestrator phase

The 6-step flow in `run_orchestrator` is deliberately fixed. Adding a new phase (e.g., a post-strategy validation step) means modifying `run_orchestrator` directly. Resist adding hooks that let strategies pre-empt the flow; that path leads to a state-graph DSL, which the project has decided not to adopt.

The intended extension point is the strategy: anything you'd want a new phase to do, do inside `Strategy.generate` using `api`.

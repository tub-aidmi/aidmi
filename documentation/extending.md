# Extending

The orchestrator is extended by registering new entries in one of four registries: providers, strategies, evaluators, fixtures. Each registry follows the same pattern: a `register_X(name, ...)` function is called at module import time. Custom entries live in user code; the orchestrator picks them up when the user imports the module before invoking the CLI or Python API.

There is no plugin discovery, no entry points, no glob-and-import. Registration is explicit.

## Writing a strategy

A strategy is any object that satisfies the `Strategy` Protocol:

```python
class Strategy(Protocol):
    name: str
    config: BaseModel
    async def generate(self, api: OrchestratorAPI) -> StrategyResult: ...
```

### Minimal example

`my_research/strategies/passthrough.py`:

```python
from pydantic import BaseModel
from aidmi_orchestrator.domain import StrategyResult
from aidmi_orchestrator.strategy.base import register_strategy, write_proposal


class PassthroughConfig(BaseModel):
    """Configuration is strategy-defined. Use a Pydantic model so the harness can validate."""
    target_table: str = "users"


class Passthrough:
    name = "passthrough"

    def __init__(self, config: PassthroughConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        # Generate trivial pass-through SQL for every source table.
        sql_by_table = {}
        source_tables = []
        for t in api.source_summary.tables:
            target = self.config.target_table
            slug = t.db_schema
            sql_by_table[target] = (
                "{{ config(materialized='table') }}\n"
                f"SELECT * FROM {{{{ source('{slug}', '{t.name}') }}}}\n"
            )
            source_tables.append((slug, t.name))

        write_proposal(
            api.dbt_project_path, sql_by_table, source_tables, api.staging_raw_dataset,
        )

        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=None,
            self_reported_status="complete",
        )


register_strategy("passthrough", Passthrough, PassthroughConfig)
```

Use it by importing the module before invoking the harness:

```python
import my_research.strategies.passthrough   # registers
from aidmi_orchestrator.strategy.base import make_strategy
strategy = make_strategy("passthrough", {"target_table": "users"})
```

Or with the CLI, by writing a thin entry script:

```python
# run_with_passthrough.py
import my_research.strategies.passthrough  # noqa: F401
from aidmi_orchestrator.cli import app
app()
```

### Using helpers from `strategy/base.py`

The base module exports helpers for tasks that strategies typically share:

| Helper | Purpose |
|--------|---------|
| `build_context_prompt(source_summary, target_schema, mode, samples_per_table)` | Builds a textual description of the source and target for the LLM. Handles the three context modes uniformly. |
| `write_proposal(dbt_project_path, sql_by_table, source_tables, raw_schema)` | Writes per-table `.sql` files and a matching `sources.yml` with literal `schema: "<raw_schema>"` entries (normalized again before each dbt run). |
| `build_manifest_from_notes(notes_by_table, strategy_name, strategy_config)` | Constructs a `MappingManifest` from per-table notes. |

Use them where they fit; ignore them if you want full control.

### Building on `strategy/structured_common.py`

Strategies that follow the structured per-table pattern (one agent call per target table, returning typed Pydantic output) should build on `strategy/structured_common.py` rather than re-implementing the scaffolding. Three helpers cover most of the work:

| Helper | Purpose |
|--------|---------|
| `make_table_agent(model, output_type, system_prompt)` | Constructs a PydanticAI `Agent` with tracing wired up, scoped to a single target table. |
| `generate_table_mapping(agent, context_prompt, table_name)` | Runs the agent for one target table and returns a `TableMapping` (includes `dbt_sql` and per-column `notes`). |
| `manifest_from_mappings(mappings, strategy_name, strategy_config)` | Collects per-table `TableMapping` results into a `MappingManifest`. |

The built-in `structured_per_table`, `write_then_critique`, `plan_then_execute`, and `ensemble_vote` strategies all use these helpers internally. A custom strategy that follows the same pattern can use them too, inheriting consistent trace events and manifest structure.

### Iteration

The orchestrator does not loop. If your strategy needs to self-correct on dbt errors, do it inside `generate`:

```python
for attempt in range(self.config.max_passes):
    # write SQL files
    transform_result = await api.run_dbt()
    if transform_result.overall_status == "success":
        break
    # inspect transform_result.models[i].error_message, rewrite SQL
```

Cap the loop with a config field. The orchestrator's final `api.run_dbt()` will run again after your strategy returns, but that final pass is what evaluators see; mid-loop dbt runs are recorded as `dbt_run` trace events for observability only.

### Using PydanticAI

`api.make_llm(spec, role)` returns a PydanticAI `Model` wrapped for tracing. Construct an `Agent` around it:

```python
from pydantic_ai import Agent

agent = Agent(
    api.make_llm(self.config.writer_model, role="writer"),
    output_type=MyPydanticOutput,
    system_prompt="...",
)
result = await agent.run(user_prompt)
return result.output
```

For tool-based strategies, see `strategy/write_tools_freeform/tools.py` for the tool-factory pattern.

## Writing an evaluator

An evaluator inspects `RunArtifacts` and returns a metric dict.

```python
from typing import Any
from aidmi_orchestrator.evaluator.base import RunArtifacts, register_evaluator


class SqlLineCount:
    """Count lines in each generated SQL file. A trivial example."""
    name = "sql_line_count"

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return (artifacts.dbt_project_path / "models").is_dir()

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        total = 0
        per_file = {}
        for sql_file in (artifacts.dbt_project_path / "models").glob("*.sql"):
            lines = sql_file.read_text().count("\n")
            per_file[sql_file.stem] = lines
            total += lines
        return {"sql_line_count_total": total, "sql_line_count_by_table": per_file}


register_evaluator("sql_line_count", SqlLineCount)
```

Metric names are free-form. Choose names that won't collide with built-in evaluators or other custom evaluators you load.

To include the evaluator in a default benchmark run, pass an explicit list:

```python
from aidmi_orchestrator.benchmark import Benchmark
from aidmi_orchestrator.cli import staging_db_url_from_env
from aidmi_orchestrator.evaluator.base import make_evaluator

staging = staging_db_url_from_env()
if staging is None:
    raise RuntimeError("configure AIDMI_STAGING_DB_URL or POSTGRES_USER/PASSWORD/DB")

bench = Benchmark(
    fixture=get_fixture("sp1_users"),
    workspace=Path("./workspace"),
    staging_db_url=staging,
    evaluators=[
        make_evaluator("execution"),
        make_evaluator("schema"),
        make_evaluator("sql_line_count"),
    ],
)
```

If you don't pass `evaluators=`, the harness reads `fixture.applicable_evaluators` and constructs the default list.

## Writing a fixture

A fixture is a Python sub-package whose `__init__.py` calls `register_fixture(...)`.

`my_research/fixtures/my_crm/__init__.py`:

```python
from pathlib import Path
from aidmi_orchestrator.fixtures.base import register_fixture, Fixture

HERE = Path(__file__).parent


def _load_source():
    # Return any dlt source. For this example, JSONL files in the same directory.
    from dlt.sources.filesystem import filesystem, read_jsonl
    return (
        filesystem(bucket_url=f"file://{HERE / 'source'}", file_glob="*.jsonl")
        | read_jsonl()
    ).with_name("contacts")


register_fixture(Fixture(
    name="my_crm",
    description="Salesforce contacts export, 2026-05-17 snapshot.",
    source_factory=_load_source,
    target_schema_path=HERE / "target_schema.json",   # optional
    reference_dbt_path=HERE / "reference_dbt",        # optional
    applicable_evaluators=["execution", "llm_usage", "schema", "row_equality"],
))
```

Data files live alongside `__init__.py`:

```
my_research/fixtures/my_crm/
├── __init__.py
├── source/
│   └── contacts.jsonl
├── target_schema.json
└── reference_dbt/
    ├── dbt_project.yml
    └── models/
        ├── sources.yml
        └── users.sql
```

For a fixture against a real live source (e.g., Salesforce), `_load_source` constructs a dlt verified source with credentials read from environment variables. No data files needed; the directory contains only `__init__.py`, the target schema, and optionally a reference dbt project.

### `target_schema.json`

A serialized `TargetSchema` Pydantic model:

```json
{
  "tables": [
    {
      "name": "users",
      "description": "Optional natural-language description shown to the LLM.",
      "primary_key": ["user_id"],
      "columns": [
        {
          "name": "user_id",
          "sql_type": "integer",
          "nullable": false,
          "description": "Optional per-column hint."
        },
        {
          "name": "status_enum",
          "sql_type": "text",
          "nullable": false,
          "enum_values": ["active", "inactive", "archived"]
        }
      ]
    }
  ]
}
```

### `reference_dbt/`

A complete, runnable dbt project that produces the ground-truth output. The `row_equality` evaluator runs this project against the same staging dataset (in a sibling schema) and compares row sets to the strategy's output. Omit this directory if no ground truth is available — the evaluator will self-skip.

## Writing an LLM provider

Providers are factory callables: `ModelSpec -> Model` (any PydanticAI `Model`).

```python
from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.llm import register_provider


def my_proxy_factory(spec: ModelSpec):
    from pydantic_ai.models.openai import OpenAIModel
    from pydantic_ai.providers.openai import OpenAIProvider
    import os
    api_key = os.environ[spec.api_key_env] if spec.api_key_env else None
    return OpenAIModel(
        spec.model_name,
        provider=OpenAIProvider(
            base_url=spec.base_url or "https://llm-proxy.internal/v1",
            api_key=api_key,
        ),
    )


register_provider("internal_proxy", my_proxy_factory)
```

A `ModelSpec` using this provider:

```yaml
writer_model:
  provider: internal_proxy
  model_name: claude-3-5-sonnet-internal
  api_key_env: INTERNAL_PROXY_KEY
```

The orchestrator does not require providers to be OpenAI-compatible. Any class that satisfies PydanticAI's `Model` interface works.

## Where to register

Place your registrations in modules you import before invoking the harness. Three reasonable patterns:

1. **A thin entry script** that imports your modules then delegates to `aidmi_orchestrator.cli`:

   ```python
   # run.py
   import my_research.strategies.passthrough  # noqa: F401
   import my_research.evaluators.line_count    # noqa: F401
   from aidmi_orchestrator.cli import app
   app()
   ```

2. **A `__init__.py` in your research package** that imports the registration modules:

   ```python
   # my_research/__init__.py
   from . import strategies, evaluators, fixtures, providers   # each subpackage registers
   ```

   Then any code that does `import my_research` triggers all registrations.

3. **Direct Python usage** when running benchmarks programmatically:

   ```python
   import my_research.strategies.passthrough
   from aidmi_orchestrator.benchmark import Benchmark
   bench = Benchmark(...)
   ```

Choose whichever fits the rest of your codebase.

## Trace events from user code

User strategies and tools record events the same way the orchestrator does: by calling `api.trace.record(...)` with a `TraceEvent` instance. The four built-in event types (`StrategyEvent`, `LlmCallEvent`, `DbtRunEvent`, `ToolCallEvent`) are defined in `aidmi_orchestrator.trace`. You may also define your own subclasses of `TraceEvent` if you need new event types; the trace reader (`TraceSink.read_all`) will skip events whose `event_type` it does not recognize, which is fine as long as downstream consumers know what to do with them.

For most strategies, `StrategyEvent(label=..., data=...)` is enough.

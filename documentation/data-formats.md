# Data formats

A run produces several files on disk. This page is the reference for each.

```
<workspace>/runs/<run-id>/
├── trace.jsonl
├── dbt_project/
├── strategy_result.json
├── mapping_manifest.json
└── result.json

<sweep-out>/
├── results.jsonl
├── sweep_config.yaml
└── dbt/
    └── <run-id>/
        └── dbt_project/
```

`<run-id>` is a [ULID](https://github.com/ulid/spec) (Crockford base32; sortable; 26 characters). Used as the directory name. Each run uses two Postgres schemas derived from the lowercase run id:

- `src_<run-id-lower>_raw` — dlt extract (source tables).
- `src_<run-id-lower>_out` — dbt models (transformed output).

`staging_raw_dataset` and `staging_out_dataset` in `result.json` record these names verbatim.

## trace.jsonl

Append-only JSONL. Each line is one event recording something the orchestrator or a strategy did. Streamed during execution; a crashed run leaves a parseable partial trace.

### Common fields

Every event has:

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO 8601 string | UTC, microsecond precision. |
| `event_type` | string | One of: `strategy`, `llm_call`, `dbt_run`, `tool_call`. Discriminator for the rest of the schema. |

### `event_type: "strategy"`

Free-form events emitted by the orchestrator and strategies to mark progress and decisions.

```json
{
  "timestamp": "2026-05-17T12:00:00",
  "event_type": "strategy",
  "label": "extract_complete",
  "data": {"rows_extracted": 8}
}
```

| Field | Type | Description |
|-------|------|-------------|
| `label` | string | Strategy-chosen identifier. Built-in labels: `extract_complete`, `discover_complete`, `strategy_crashed`, `final_dbt_failed`. Strategies are free to emit their own. |
| `data` | object | Arbitrary JSON-serializable payload. |

### `event_type: "llm_call"`

Recorded by `TracedModel` for every LLM call. Strategies do not emit these directly.

```json
{
  "timestamp": "2026-05-17T12:00:01",
  "event_type": "llm_call",
  "role": "writer",
  "model_spec": {
    "provider": "openai",
    "model_name": "gpt-4o-mini",
    "base_url": null,
    "api_key_env": "OPENAI_API_KEY",
    "extra": {}
  },
  "messages": [{"kind": "request", "parts": [...]}],
  "response": {"kind": "response", "parts": [...]},
  "usage": {
    "input_tokens": 1234,
    "output_tokens": 567,
    "cache_read_tokens": 0,
    "cache_write_tokens": 0
  },
  "latency_ms": 1234.5
}
```

| Field | Type | Description |
|-------|------|-------------|
| `role` | string | Free-form label the strategy passes to `api.make_llm(spec, role=...)`. Used by `LlmUsageEvaluator` to break down cost by role. Examples: `writer`, `critic`, `per_table[contacts]`. |
| `model_spec` | object | The `ModelSpec` as provided by the strategy. `api_key_env` is the env var name, never the key itself. |
| `messages` | array | PydanticAI request messages, serialized via `model_dump()` when possible. Lossless for downstream introspection. |
| `response` | object or string | PydanticAI response, serialized via `model_dump()` when possible. |
| `usage` | object | Normalized token counts. PydanticAI flattens provider-specific cache fields (OpenAI `cached_tokens`, Anthropic `cache_read_input_tokens`) into `cache_read_tokens`. |
| `latency_ms` | number | Wall-clock duration of the model request in milliseconds. |

### `event_type: "dbt_run"`

Recorded when a strategy calls `api.run_dbt()` and when the orchestrator runs the final canonical dbt pass.

```json
{
  "timestamp": "2026-05-17T12:01:00",
  "event_type": "dbt_run",
  "transform_result": {
    "models": [
      {"model_name": "users", "status": "success", "error_message": null, "rows_affected": null, "execution_time_seconds": 1.234}
    ],
    "overall_status": "success"
  },
  "duration_ms": 5678.9
}
```

| Field | Type | Description |
|-------|------|-------------|
| `transform_result` | object | Serialized `aidmi_pipeline.migration.TransformResult`. See SP1's pipeline package for the schema. |
| `duration_ms` | number | Wall-clock duration of the dbt invocation. |

### `event_type: "tool_call"`

Recorded when a strategy uses a tool provided by the orchestrator API (e.g., `write_file`, `read_file`, `query_postgres`) or a user-defined tool that opts in to tracing.

```json
{
  "timestamp": "2026-05-17T12:00:30",
  "event_type": "tool_call",
  "tool_name": "write_file",
  "args": {"path": "models/users.sql", "size": 412},
  "result": "ok",
  "latency_ms": 0.8
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tool_name` | string | The tool's registered name as the agent sees it. |
| `args` | object | Tool arguments. The tool factory chooses what to log (e.g., file size instead of full content). |
| `result` | any | Tool return value or summary. |
| `latency_ms` | number | Tool execution time. |

### Reading a trace in Python

```python
from aidmi_orchestrator.trace import TraceSink
events = TraceSink.read_all(Path("aidmi_workspace/runs/01HXX.../trace.jsonl"))
for ev in events:
    print(type(ev).__name__, ev.timestamp)
```

`read_all` deserializes each line into the appropriate Pydantic subclass based on `event_type`.

## strategy_result.json

The strategy's self-reported summary, written verbatim from the `StrategyResult` Pydantic model.

```json
{
  "target_tables_written": ["users"],
  "target_schema": { ... },
  "manifest": null,
  "self_reported_status": "complete"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `target_tables_written` | array of strings | Names the strategy claims it produced. Independent of what actually appeared in Postgres. |
| `target_schema` | object or null | The schema the strategy committed to. May be the input target echoed back, an LLM-designed schema, or null. |
| `manifest` | object or null | `MappingManifest` if the strategy produced one. See below. |
| `self_reported_status` | enum | `complete`, `partial`, or `gave_up`. |

## mapping_manifest.json

Optional. Strategies that produce structured per-column mapping notes emit this; tool-based strategies that only write SQL typically do not. The format is `MappingManifest`:

```json
{
  "tables": [
    {
      "target_table": "users",
      "source_tables": ["contacts"],
      "column_notes": [
        {
          "target_column": "user_id",
          "source_columns": ["id"],
          "explanation": "Pass-through identifier"
        },
        {
          "target_column": "email_address",
          "source_columns": ["email"],
          "explanation": "Trim and lowercase"
        }
      ],
      "reasoning": "Direct one-to-one mapping with light normalization."
    }
  ],
  "strategy_name": "structured_per_table",
  "strategy_config": { ... }
}
```

The manifest is human-readable documentation of the strategy's intent. Evaluators do not trust it; they verify by introspecting the post-dbt Postgres state.

## result.json

The `BenchmarkResult` for a single run.

```json
{
  "run_id": "01HXX0000000000000000000",
  "fixture_name": "sp1_users",
  "strategy_name": "mock",
  "strategy_spec_name": "mock",
  "strategy_config": { ... },
  "started_at": "2026-05-17T12:00:00",
  "completed_at": "2026-05-17T12:01:00",
  "wall_clock_seconds": 60.0,
  "strategy_result": { ... },
  "metrics": { ... },
  "error": null,
  "staging_raw_dataset": "src_01hxx00000000000000000000_raw",
  "staging_out_dataset": "src_01hxx00000000000000000000_out"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | string | ULID, matches the directory name. |
| `fixture_name` | string | The fixture this run targeted. |
| `strategy_name` | string | Registered strategy implementation (`mock`, `structured_per_table`, `write_tools_freeform`, …). |
| `strategy_spec_name` | string | Label for the exact spec (YAML `name` for `run`, or grid cell `name` with optional cartesian suffixes for `sweep`). |
| `strategy_config` | object | Full serialized config, including `ModelSpec`s but never API keys. |
| `started_at`, `completed_at` | ISO 8601 | UTC timestamps. |
| `wall_clock_seconds` | number | End-to-end run duration. |
| `strategy_result` | object | The strategy's own `StrategyResult` (mirrored in `strategy_result.json` for convenience). |
| `metrics` | object | All evaluator outputs merged. Schema is free-form; each evaluator contributes its own keys. |
| `error` | string or null | Populated when the orchestrator caught a strategy crash. The run still produces a `result.json`; evaluators that can run on partial state still execute. |
| `staging_raw_dataset` | string | Postgres schema for extract (e.g. `src_01abc…_raw`). Empty when the run failed before artifacts were built. |
| `staging_out_dataset` | string | Postgres schema for dbt output (e.g. `src_01abc…_out`). Empty when the run failed before artifacts were built. |

### Default metric keys

The four built-in evaluators contribute the following:

| Key | Source | Description |
|-----|--------|-------------|
| `dbt_success` | execution | `true` if the final dbt run had `overall_status: success`. |
| `dbt_models_succeeded`, `dbt_models_failed` | execution | Per-model counts. |
| `dbt_error_messages` | execution | List of error strings from failed models. |
| `strategy_status` | execution | Echo of `strategy_result.self_reported_status`. |
| `llm_calls_total`, `llm_calls_by_role` | llm_usage | Counts. |
| `tokens_input_total`, `tokens_input_by_role`, `tokens_input_cached`, `tokens_input_uncached`, `tokens_output_total`, `tokens_output_by_role` | llm_usage | Token sums. |
| `cache_hit_rate` | llm_usage | Cached / total input tokens, in `[0, 1]`. |
| `dollar_cost_total`, `dollar_cost_by_role` | llm_usage | Computed via LiteLLM `model_cost` + optional override. `null` if no pricing data found. |
| `latency_ms_by_role`, `latency_ms_sum_by_role` | llm_usage | Per-role mean and sum of call latencies (ms). |
| `latency_ms_total` | llm_usage | Sum of all LLM call latencies. |
| `latency_ms_p50_by_role`, `latency_ms_p95_by_role` | llm_usage | Per-role percentiles. |
| `produced_column_count`, `produced_type_histogram` | schema | Always emitted. |
| `target_columns_covered`, `extraneous_columns`, `type_mismatches` | schema | Populated when a target schema is available (input or strategy-claimed). |
| `row_count_match` | row_equality | `true` if all produced tables have the same row count as the reference. |
| `row_set_diff_count` | row_equality | Number of rows that differ across all tables. |
| `per_table_equality` | row_equality | Per-target-table dict containing `row_count_match`, `row_set_diff_count`, `produced_rows`, `reference_rows`, `column_value_match_rate`. |
| `any_table_mismatch` | row_equality | `true` if any table had a row-count or row-set mismatch. |

User-supplied evaluators contribute additional keys. Keys are free-form; consumers should treat missing keys as "this evaluator did not apply to this run".

## results.jsonl

Sweep output. One `BenchmarkResult` per line, in the order cells completed. Streaming append: a crashed sweep leaves a parseable partial file.

Pandas:

```python
import pandas as pd
df = pd.read_json("sweep-out/results.jsonl", lines=True)
df_metrics = pd.json_normalize(df["metrics"])
df = pd.concat([df.drop("metrics", axis=1), df_metrics], axis=1)
```

## sweep_config.yaml

A verbatim copy of the grid YAML that produced the sweep. Written once at sweep start. Preserves reproducibility.

## mock_mapping.json

Format consumed by the `mock` strategy.

```json
{
  "tables": {
    "users": {
      "sql": "{{ config(materialized='table') }}\nSELECT id AS user_id, ... FROM {{ source('src_crm', 'contacts') }}",
      "source_tables": [["src_crm", "contacts"]],
      "column_notes": [
        {
          "target_column": "user_id",
          "source_columns": ["id"],
          "explanation": "Pass-through"
        }
      ],
      "reasoning": "Static mapping equivalent to the SP1 reference."
    }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tables` | object | Keys are target table names; values are per-table mapping blocks. |
| `tables.<name>.sql` | string | Full dbt model SQL. Written verbatim to `models/<name>.sql`. |
| `tables.<name>.source_tables` | array of `[schema, name]` pairs | Used to generate `sources.yml`. |
| `tables.<name>.column_notes` | array | Used to populate the manifest. |
| `tables.<name>.reasoning` | string | Used in the manifest. |

# Configuration

The orchestrator is driven by three YAML/JSON formats: strategy specs, grid files, and an optional pricing override. This page is the reference for each.

## Strategy spec YAML

A strategy spec describes one `(strategy, config)` pair. Consumed by `aidmi-orchestrator run --strategy-spec <path>` (via `parse_strategy_spec()` + `make_strategy()` in Python) or by calling `make_strategy(name, config_dict)` directly when you build config in code.

### Schema

```yaml
name: <unique label for this spec>        # required for file-based runs; convention: match YAML filename stem
strategy: <registered strategy name>      # required
config:                                   # strategy-defined schema; see below
  ...
```

The `name` field is recorded in each run's `result.json` as `strategy_spec_name` so you can tell which YAML produced a row (for example two files that both use `strategy: structured_per_table`). It is **not** the registry name: that remains `strategy_name` (`structured_per_table`, `write_tools_freeform`, etc.).

The `config` block is validated against the strategy's Pydantic config class. Unknown fields raise a validation error.

### Built-in strategy configs

#### `mock`

```yaml
name: mock
strategy: mock
config:
  mapping_source: <path to mapping JSON>   # required, file readable at run time
```

The mapping JSON must conform to the schema in [Data formats — mock_mapping.json](data-formats.md#mock-mapping-json).

#### `structured_per_table`

```yaml
name: structured_per_table_openai_example
strategy: structured_per_table
config:
  writer_model:                       # required, see ModelSpec below
    provider: openai
    model_name: gpt-4o-mini
    api_key_env: OPENAI_API_KEY
  context_mode: metadata_plus_samples  # one of metadata_only, metadata_plus_samples, live_query_tool
  samples_per_table: 3                 # int, default 3 (ignored unless mode includes samples)
  max_query_tool_rows: 100             # int, default 100 (ignored unless mode is live_query_tool)
  enable_self_correction: false        # bool, default false; re-runs dbt after each table pass
  max_self_correction_passes: 3        # int, default 3; ignored unless enable_self_correction
  serial_llm_calls: false              # bool, default false; await per-table LLM work one at a time
```

When `serial_llm_calls` is `true`, per-table writer calls (and self-correction regenerations) run sequentially instead of via `asyncio.gather`. Use this for local Ollama or other single-slot backends. Pair with `concurrency: 1` in grid YAML so the sweep itself does not run cells in parallel.

#### `write_tools_freeform`

```yaml
name: write_tools_freeform_example
strategy: write_tools_freeform
config:
  writer_model:                        # required
    provider: anthropic
    model_name: claude-3-5-sonnet-latest
    api_key_env: ANTHROPIC_API_KEY
  context_mode: metadata_plus_samples
  samples_per_table: 3
  max_query_tool_rows: 100
  max_tool_turns: 20                   # int, default 20; cap on agent reasoning turns
  enable_self_correction: false        # bool, default false
  inline_run_dbt_tool: false           # bool, default false; expose run_dbt during agent turns
  max_self_correction_passes: 3        # int, default 3; ignored unless enable_self_correction
```

#### `write_then_critique`

```yaml
name: write_then_critique_example
strategy: write_then_critique
config:
  writer_model:                        # required
    provider: litellm
    model_name: my-model
    api_key_env: LITELLM_API_KEY
  critic_model:                        # optional; defaults to writer_model when omitted
    provider: litellm
    model_name: my-critic-model
    api_key_env: LITELLM_API_KEY
  max_critique_rounds: 2               # int ≥ 1, default 2; alternates write/critique passes
  serial_llm_calls: false              # bool, default false; await per-table LLM work one at a time
```

The writer produces an initial dbt project; the critic reviews the SQL for correctness and mapping quality, returning structured feedback. The writer then revises up to `max_critique_rounds` times. This strategy also accepts `context_mode`, `samples_per_table`, `max_query_tool_rows`, and `serial_llm_calls` with the same defaults as `structured_per_table`.

#### `plan_then_execute`

```yaml
name: plan_then_execute_example
strategy: plan_then_execute
config:
  planner_model:                       # required
    provider: litellm
    model_name: my-model
    api_key_env: LITELLM_API_KEY
  writer_model:                        # optional; defaults to planner_model when omitted
    provider: litellm
    model_name: my-writer-model
    api_key_env: LITELLM_API_KEY
  serial_llm_calls: false              # bool, default false; await per-table LLM work one at a time
```

The planner produces a structured mapping plan (table-by-table column assignments); the writer turns the plan into dbt SQL. Using a cheaper/smaller writer model with a stronger planner is a common configuration. This strategy also accepts `context_mode`, `samples_per_table`, `max_query_tool_rows`, and `serial_llm_calls` with the same defaults as `structured_per_table`.

#### `ensemble_vote`

```yaml
name: ensemble_vote_example
strategy: ensemble_vote
config:
  writer_model:                        # required
    provider: litellm
    model_name: my-model
    api_key_env: LITELLM_API_KEY
  judge_model:                         # optional; defaults to writer_model when omitted
    provider: litellm
    model_name: my-judge-model
    api_key_env: LITELLM_API_KEY
  n_candidates: 3                      # int ≥ 1, default 3; independent generation passes
  serial_llm_calls: false              # bool, default false; await per-candidate and per-table LLM work one at a time
```

Runs `n_candidates` independent write passes with the writer model, then has the judge select the best candidate per target table. This strategy also accepts `context_mode`, `samples_per_table`, `max_query_tool_rows`, and `serial_llm_calls` with the same defaults as `structured_per_table`.

### `ModelSpec`

Used by every LLM-driven strategy as the `writer_model` (or other role-named) field.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | string | yes | Name of a registered provider. Built-ins: `openai`, `openai_compatible`, `ollama`, `anthropic`, `litellm`, `google_cloud`. |
| `model_name` | string | yes | Model identifier the provider accepts (e.g., `gpt-4o-mini`, `claude-3-5-sonnet-latest`, `llama3:70b`, `gemini-2.5-flash`). |
| `base_url` | string | no | Override the provider's default base URL. Useful for OpenAI-compatible proxies and Ollama. |
| `api_key_env` | string | no | Name of the environment variable holding the API key. The key itself is never serialized. Omit or leave unset for providers that require no key (see Ollama). |
| `extra` | object | no | Provider-specific options. For `google_cloud` ADC auth: `project`, `location`. |

### Examples

OpenAI:
```yaml
writer_model:
  provider: openai
  model_name: gpt-4o-mini
  api_key_env: OPENAI_API_KEY
```

Anthropic:
```yaml
writer_model:
  provider: anthropic
  model_name: claude-3-5-sonnet-latest
  api_key_env: ANTHROPIC_API_KEY
```

Local Ollama (no API key — default `base_url` in code is `http://localhost:11434` if omitted; `/v1` is appended automatically for the OpenAI-compatible chat API):
```yaml
writer_model:
  provider: ollama
  model_name: llama3:70b
  base_url: http://localhost:11434   # optional
```

LiteLLM proxy (OpenAI-compatible API); set host in YAML (`api_key_env` matches [`.env.example`](../.env.example)):
```yaml
writer_model:
  provider: litellm
  model_name: gpt-4o-mini
  base_url: http://localhost:4000/v1
  api_key_env: LITELLM_API_KEY
```

A self-hosted OpenAI-compatible endpoint (vLLM, internal gateways; same pattern as LiteLLM for URL + key naming):
```yaml
writer_model:
  provider: openai_compatible
  model_name: my-tuned-model
  base_url: https://llm.internal/v1
  api_key_env: OPENAI_COMPATIBLE_API_KEY
```

Google Agent Platform / Vertex AI (API key from Cloud Console — no project or location required):
```yaml
writer_model:
  provider: google_cloud
  model_name: gemini-2.5-flash
  api_key_env: GOOGLE_API_KEY
```

Google Cloud with Application Default Credentials (service account or `gcloud auth application-default login`):
```yaml
writer_model:
  provider: google_cloud
  model_name: gemini-2.5-flash
  extra:
    project: my-gcp-project
    location: us-central1
```

Set `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_LOCATION` in [`.env.example`](../.env.example) instead of `extra` when the same project/region applies to every model in a run.

## Grid YAML

A grid YAML describes a sweep — multiple `(strategy, config)` cells run against one fixture. Consumed by `aidmi-orchestrator sweep --grid <path>` and by `expand_grid()` in Python.

### Schema

```yaml
fixture: <fixture name or list>       # string or list; CLI --fixture overrides; list runs each fixture
runs_per_cell: <int>                  # optional, default 1
concurrency: <int>                    # optional, default 3; max parallel runs
per_model_exclusive: <bool>           # optional, default false; when true, at most one in-flight job
                                      # per distinct model_name (ignores exclusive_model_prefixes grouping)
exclusive_model_prefixes: [<prefix>]  # optional, default ["ise-"]; models whose names start with any
                                      # prefix run one-at-a-time within that prefix group (others are
                                      # passthrough and run in parallel up to concurrency)
models:                               # optional; named ModelSpec definitions
  <ref>:
    provider: <name>
    model_name: <name>
    base_url: <url>                   # optional
    api_key_env: <env var name>       # optional
cells:                                # required
  - name: <optional label>              # used as strategy_spec_name base; default: same as strategy
    strategy: <name>
    fixtures: [<name>, ...]           # optional; restrict this cell to a subset of the sweep fixtures
    config: { ... }
  - strategy: <name>
    config: { ... }
```

**Top-level `fixture`** may be a string (single fixture) or a YAML list (multi-fixture). The sweep runs every cell against every fixture unless the cell has its own `fixtures:` restriction.

**`models:` named-spec block.** Define ModelSpec dicts once under `models:` and reference them by name in any `*_model` config field. A list of refs in a `*_model` field expands cartesian-wise just like any other list field (see below):

```yaml
models:
  small:
    provider: litellm
    model_name: ise-ollama/small-model
    base_url: http://localhost:4000/v1
    api_key_env: LITELLM_API_KEY
  large:
    provider: litellm
    model_name: ise-ollama/large-model
    base_url: http://localhost:4000/v1
    api_key_env: LITELLM_API_KEY
cells:
  - name: structured
    strategy: structured_per_table
    config:
      writer_model: [small, large]    # expands into 2 cells, one per model
      context_mode: metadata_plus_samples
```

**`exclusive_model_prefixes`** controls model-major scheduling. Models whose `model_name` starts with any listed prefix (e.g., `ise-`) are loaded one at a time: the sweep serializes all jobs for one exclusive model before starting the next. Models that do not match any prefix run in parallel up to `concurrency` with no serialization constraint.

**`per_model_exclusive`** (alternative scheduling mode) caps each distinct `model_name` to one in-flight job at a time while still allowing up to `concurrency` jobs overall — useful when you have N models and want N parallel workers with no two jobs sharing the same model. When enabled, prefix-based exclusive grouping is skipped; set `exclusive_model_prefixes: []` if you rely solely on per-model locks.

### Cartesian expansion

A cell whose config has list-valued top-level scalar fields expands into the cartesian product of those lists. Lists in nested objects (e.g., `writer_model.base_url`) are not expanded; treat them as literal list values.

Example:

```yaml
cells:
  - strategy: structured_per_table
    config:
      writer_model:
        provider: openai
        model_name: gpt-4o-mini
        api_key_env: OPENAI_API_KEY
      context_mode: [metadata_only, metadata_plus_samples]
      samples_per_table: [3, 10]
```

expands into 4 cells (2 × 2). When a cell expands this way, each output row gets a distinct `strategy_spec_name` by appending `_<key>_<value>` fragments (lowercased, punctuation to underscores) for each varied dimension, prefixed by the cell `name` when set, otherwise by the registry strategy name.

Example: with `name: spt` and `context_mode: [metadata_only, …]`, labels look like `spt_context_mode_metadata_only`.

To sweep across nested fields like `model_name` without using the `models:` block, write a separate cell block per value:

```yaml
cells:
  - strategy: structured_per_table
    config:
      writer_model: { provider: openai, model_name: gpt-4o-mini, api_key_env: OPENAI_API_KEY }
      context_mode: metadata_plus_samples
  - strategy: structured_per_table
    config:
      writer_model: { provider: openai, model_name: gpt-4o,      api_key_env: OPENAI_API_KEY }
      context_mode: metadata_plus_samples
```

### Complete example

```yaml
fixture: [mock, master]
runs_per_cell: 3
concurrency: 3
exclusive_model_prefixes: ["ise-"]
models:
  qwen9b:
    provider: litellm
    model_name: ise-openai-nvidia/qwen35-9b
    base_url: http://localhost:4000/v1
    api_key_env: LITELLM_API_KEY
cells:
  - name: mock_control
    strategy: mock
    fixtures: [mock]
    config:
      mapping_source: packages/orchestrator/src/aidmi_orchestrator/fixtures/mock/mock_mapping.json
  - name: structured
    strategy: structured_per_table
    config:
      writer_model: [qwen9b]
      context_mode: metadata_plus_samples
```

Total: 1 (mock, mock fixture only) + 1×2 fixtures = 3 cells. With `runs_per_cell: 3`, 9 benchmark runs.

## Pricing override

Costs in `BenchmarkResult.metrics.dollar_cost_total` are computed from LiteLLM's `model_cost` table by default. For models LiteLLM does not know (custom Ollama tags, internal proxies, recently released models), supply a JSON file via `--pricing-config` or by placing it at `packages/orchestrator/configs/pricing.json`.

### Schema

```json
{
  "<provider>/<model_name>": {
    "input_cost_per_token": 0.0,
    "output_cost_per_token": 0.0,
    "cached_input_cost_per_token": 0.0
  },
  "another/model": { ... }
}
```

All three fields are dollars per token. `cached_input_cost_per_token` is optional; if absent, the cached portion is priced at the same rate as uncached input (an overestimate of true cost).

### Resolution order

For a `ModelSpec` with provider `P` and model name `M`, the lookup order is:

1. Override file entry keyed `P/M`.
2. LiteLLM's `model_cost[M]` (LiteLLM's bare-name key).
3. LiteLLM's `model_cost[P/M]` (provider-prefixed key, where LiteLLM uses one).
4. None — the call is recorded in the trace but contributes `null` to `dollar_cost_total`. A warning event is emitted.

### Example override

For a local Ollama model that LiteLLM does not price:

```json
{
  "ollama/llama3:70b": {
    "input_cost_per_token": 0.0,
    "output_cost_per_token": 0.0
  }
}
```

For a corporate proxy with a different price structure:

```json
{
  "corporate/internal-llm-v2": {
    "input_cost_per_token": 0.00001,
    "output_cost_per_token": 0.00002,
    "cached_input_cost_per_token": 0.000001
  }
}
```

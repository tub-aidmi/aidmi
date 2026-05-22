# Configuration

The orchestrator is driven by three YAML/JSON formats: strategy specs, grid files, and an optional pricing override. This page is the reference for each.

## Strategy spec YAML

A strategy spec describes one `(strategy, config)` pair. Consumed by `aidmi-orchestrator run --strategy-spec <path>` and by `make_strategy(name, config_dict)` in Python.

### Schema

```yaml
strategy: <registered strategy name>      # required
config:                                   # strategy-defined schema; see below
  ...
```

The `config` block is validated against the strategy's Pydantic config class. Unknown fields raise a validation error.

### Built-in strategy configs

#### `mock`

```yaml
strategy: mock
config:
  mapping_source: <path to mapping JSON>   # required, file readable at run time
```

The mapping JSON must conform to the schema in [Data formats — mock_mapping.json](data-formats.md#mock-mapping-json).

#### `structured_per_table`

```yaml
strategy: structured_per_table
config:
  writer_model:                       # required, see ModelSpec below
    provider: openai
    model_name: gpt-4o-mini
    api_key_env: OPENAI_API_KEY
  context_mode: metadata_plus_samples  # one of metadata_only, metadata_plus_samples, live_query_tool
  samples_per_table: 3                 # int, default 3 (ignored unless mode includes samples)
  max_query_tool_rows: 100             # int, default 100 (ignored unless mode is live_query_tool)
```

#### `write_tools_freeform`

```yaml
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
  max_self_correction_passes: 3        # int, default 3; ignored unless enable_self_correction
```

### `ModelSpec`

Used by every LLM-driven strategy as the `writer_model` (or other role-named) field.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | string | yes | Name of a registered provider. Built-ins: `openai`, `openai_compatible`, `ollama`, `anthropic`, `litellm`. |
| `model_name` | string | yes | Model identifier the provider accepts (e.g., `gpt-4o-mini`, `claude-3-5-sonnet-latest`, `llama3:70b`). |
| `base_url` | string | no | Override the provider's default base URL. Useful for OpenAI-compatible proxies and Ollama. |
| `api_key_env` | string | no | Name of the environment variable holding the API key. The key itself is never serialized. Omit or leave unset for providers that require no key (see Ollama). |
| `extra` | object | no | Provider-specific options. Passed through opaquely. |

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

Local Ollama (no API key — default `base_url` in code is `http://localhost:11434` if omitted):
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

## Grid YAML

A grid YAML describes a sweep — multiple `(strategy, config)` cells run against one fixture. Consumed by `aidmi-orchestrator sweep --grid <path>` and by `expand_grid()` in Python.

### Schema

```yaml
fixture: <fixture name>               # optional; CLI flag wins if set
runs_per_cell: <int>                  # optional, default 1
cells:                                # required
  - strategy: <name>
    config: { ... }
  - strategy: <name>
    config: { ... }
```

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

expands into 4 cells (2 × 2). To sweep across nested fields like `model_name`, write a separate cell block per value:

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
fixture: sp1_users
runs_per_cell: 1
cells:
  - strategy: mock
    config:
      mapping_source: packages/orchestrator/src/aidmi_orchestrator/fixtures/sp1_users/mock_mapping.json
  - strategy: structured_per_table
    config:
      writer_model:
        provider: openai
        model_name: gpt-4o-mini
        api_key_env: OPENAI_API_KEY
      context_mode: [metadata_only, metadata_plus_samples, live_query_tool]
      samples_per_table: 3
  - strategy: write_tools_freeform
    config:
      writer_model:
        provider: anthropic
        model_name: claude-3-5-sonnet-latest
        api_key_env: ANTHROPIC_API_KEY
      context_mode: metadata_plus_samples
      enable_self_correction: [false, true]
```

Total: 1 + 3 + 2 = 6 cells.

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

# CLI reference

The orchestrator ships a Typer-based CLI installed as `aidmi-orchestrator`. Two subcommands: `run` and `sweep`.

## Environment

Staging Postgres resolves in this order:

| Variable(s) | Required | Description |
|-------------|----------|-------------|
| `AIDMI_STAGING_DB_URL` | No* | Overrides everything when set (non-empty). Format: `postgresql://user:pass@host:port/dbname`. |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | No* | When `AIDMI_STAGING_DB_URL` is unset, the CLI builds `postgresql://user:password@host:port/db`. User/password are URL-encoded automatically. |
| `POSTGRES_HOST` | No | Defaults to `localhost` when composing the URL. |
| `POSTGRES_PORT` | No | Defaults to `5432` when composing the URL. |

\*Either set `AIDMI_STAGING_DB_URL`, or all three `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`. [`.env.example`](../.env.example) follows the compose form (no duplicated URL unless you uncomment the override).

Strategy specs and grid YAML choose `provider`, model, optional `base_url`, and optional `api_key_env`. Only the referenced API keys typically belong in `.env` ([`.env.example`](../.env.example)); host URLs belong in YAML (see [`configuration.md`](configuration.md)).

If a `.env` file exists in the current working directory when you invoke `aidmi-orchestrator`, it is loaded automatically via `python-dotenv`. Existing shell-defined variables are **not** overridden.

## `aidmi-orchestrator run`

Execute one orchestrator pass against a fixture with one strategy.

```
aidmi-orchestrator run \
  --fixture NAME \
  --strategy-spec PATH \
  [--run-id ID] \
  [--workspace DIR]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--fixture` | required | Name of a registered fixture (e.g., `sp1_users`). |
| `--strategy-spec` | required | Path to a YAML file describing the strategy and its config. See [Configuration](configuration.md). |
| `--run-id` | auto (ULID) | Optional run identifier. Used as the directory name under `<workspace>/runs/` and as the staging schema suffix (`src_<run-id>`). |
| `--workspace` | `./aidmi_workspace` | Directory where per-run artifacts are written. |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Run completed. Note that `dbt_success=false` in the result is not an exit-code-0 failure; the run "completed" even if the SQL failed to compile. |
| Non-zero | Run could not start (missing env var, unknown fixture, malformed spec) or the strategy raised an unhandled exception. |

### Output

The full `BenchmarkResult` is printed to stdout as indented JSON. Artifacts on disk:

```
<workspace>/runs/<run-id>/
├── trace.jsonl
├── dbt_project/
├── strategy_result.json
├── mapping_manifest.json   # if the strategy produced one
└── result.json
```

## `aidmi-orchestrator sweep`

Run multiple `(strategy, config)` cells against one fixture and stream `BenchmarkResult` rows to a JSONL file.

```
aidmi-orchestrator sweep \
  --grid PATH \
  --out DIR \
  [--fixture NAME] \
  [--runs-per-cell N] \
  [--workspace DIR]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--grid` | required | Path to a grid YAML file. See [Configuration](configuration.md#grid-yaml). |
| `--out` | required | Output directory for sweep results. |
| `--fixture` | from grid | Fixture name. Falls back to the grid YAML's top-level `fixture:` key if not given as a flag. An explicit flag overrides the YAML. |
| `--runs-per-cell` | from grid, then 1 | Number of repetitions per cell. Falls back to the grid YAML's top-level `runs_per_cell:` key, then to 1. |
| `--workspace` | `./aidmi_workspace` | Workspace directory. |

### Cartesian expansion

A grid cell whose config has a list-valued top-level scalar field expands into multiple cells. For example:

```yaml
cells:
  - strategy: structured_per_table
    config:
      writer_model:
        provider: openai
        model_name: gpt-4o-mini
        api_key_env: OPENAI_API_KEY
      context_mode: [metadata_only, metadata_plus_samples, live_query_tool]
```

expands into three cells, one per `context_mode` value. The expansion applies to top-level scalar fields only; nested fields like `writer_model.model_name` are not expanded. To sweep models, write a separate cell block for each model.

### Output

```
<out>/
├── results.jsonl       # one BenchmarkResult per line, streamed as cells complete
└── sweep_config.yaml   # the grid that was run, for reproducibility
```

Each run's full per-cell artifacts (trace, dbt project, result.json) are also written under `<workspace>/runs/<run-id>/` so individual cells can be inspected offline.

### Failure handling

A cell that fails (strategy crash, dbt error, missing API key) does not stop the sweep. The cell's `BenchmarkResult` is written with `error` set to a string describing the failure, and the next cell starts. The `results.jsonl` always contains exactly one row per cell.

A `KeyboardInterrupt` or other signal stops the sweep at the next cell boundary. Already-completed cells remain in `results.jsonl`.

## Examples

Run the mock strategy on the SP1 fixture (typically `make up` from repo root plus a `.env` matching [`.env.example`](../.env.example)):

```bash
aidmi-orchestrator run \
  --fixture sp1_users \
  --strategy-spec packages/orchestrator/examples/strategy_specs/mock.yaml
```

Sweep all three context modes of `structured_per_table` against `sp1_users` with two repetitions per cell:

```bash
export OPENAI_API_KEY=sk-...    # matches api_key_env in the grid YAML
aidmi-orchestrator sweep \
  --grid packages/orchestrator/examples/day1_grid.yaml \
  --out aidmi_workspace/results/2026-05-17-day1 \
  --runs-per-cell 2
```

Run a custom strategy from your own code:

```bash
# Register your strategy module before invoking the CLI.
uv run python -c "
import my_research_strategies   # registers 'my_strategy' on import
import aidmi_orchestrator.cli as cli
import sys
sys.argv = ['aidmi-orchestrator', 'run', '--fixture', 'sp1_users',
            '--strategy-spec', 'my_strategy_spec.yaml']
cli.app()
"
```

See [Extending](extending.md) for the registration pattern.

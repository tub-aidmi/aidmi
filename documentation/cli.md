# CLI reference

The orchestrator ships a Typer-based CLI installed as `aidmi-orchestrator`. Three subcommands: `run`, `sweep`, and `report`.

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

If a `.env` file exists in the current working directory when you invoke `aidmi-orchestrator`, it is loaded automatically via `python-dotenv` (file values override shell exports).

## `aidmi-orchestrator run`

Execute one orchestrator pass against a fixture with one strategy.

```
aidmi-orchestrator run \
  --fixture NAME \
  --strategy-spec PATH \
  [--run-id ID] \
  [--workspace DIR] \
  [-v | --verbose]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--fixture` | required | Name of a registered fixture (e.g., `sp1_users`). |
| `--strategy-spec` | required | Path to YAML with required `name` (written to results as `strategy_spec_name`), `strategy` (registry id), and `config`. See [Configuration](configuration.md). |
| `--run-id` | auto (ULID) | Optional run identifier. Used as the directory name under `<workspace>/runs/` and to derive Postgres schemas `src_<run-id-lower>_raw` and `src_<run-id-lower>_out`. |
| `--workspace` | `./aidmi_workspace` | Directory where per-run artifacts are written. |
| `--verbose`, `-v` | off | Streams each [`trace.jsonl`](data-formats.md#tracejsonl) record to stderr as it is appended (same JSON lines written on disk). |

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

For live progress during a single run: pass `-v` / `--verbose` (duplicate trace lines on stderr). With a pinned `--run-id`, you can also [`tail`](https://manpages.debian.org/stable/coreutils/tail.1.en.html)-follow `trace.jsonl` ([`sweep`](#live-trace-tail--verbose) shows a [`jq`](https://jqlang.org/) filter example).

Run multiple `(strategy, config)` cells across one or more fixtures and stream `BenchmarkResult` rows to a JSONL file.

```
aidmi-orchestrator sweep \
  --grid PATH \
  --out DIR \
  [--fixture NAME] \
  [--runs-per-cell N] \
  [--concurrency N] \
  [--resume | --no-resume] \
  [--workspace DIR] \
  [-v | --verbose]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--grid` | required | Path to a grid YAML file. See [Configuration](configuration.md#grid-yaml). |
| `--out` | required | Output directory for sweep results. |
| `--fixture` | from grid | Single fixture override. Falls back to the grid YAML's `fixture:` key (string or list). The YAML may list multiple fixtures; `--fixture` collapses to a single fixture and overrides the YAML. |
| `--runs-per-cell` | from grid, then 1 | Number of repetitions per cell. Falls back to the grid YAML's `runs_per_cell:` key, then to 1. Note: the value 1 acts as "unset" — `--runs-per-cell 1` cannot override a grid file's `runs_per_cell` greater than 1. To force single runs, edit the grid file or use a copy with `runs_per_cell: 1`. |
| `--concurrency` | from grid, then 3 | Maximum number of parallel runs. Falls back to the grid YAML's `concurrency:` key, then to 3. |
| `--resume` / `--no-resume` | `--resume` | When `--resume` (default), skip any `(spec, fixture, rep)` tuple whose result row already exists in `results.jsonl`. `--no-resume` truncates the file and re-runs everything. |
| `--workspace` | `./aidmi_workspace` | Workspace directory. |
| `--verbose`, `-v` | off | Same as [`run`](#aidmi-orchestrator-run): stream trace JSON lines to stderr. Only active when `--concurrency 1`; a warning is printed and mirroring is suppressed otherwise. |

### Rep-major scheduling

Jobs expand in **rep → fixture → cell** order: for each repetition, every cell runs on one fixture before moving to the next fixture, then the next repetition starts. That way an interrupted sweep still has every cell at rep 0, and results for one fixture are complete across the strategy/model grid before the next fixture starts.

### Model-major scheduling

The sweep schedules jobs in model-major order for models whose `model_name` starts with any prefix listed in the grid's `exclusive_model_prefixes` (default `["ise-"]`). All jobs for one exclusive model finish before jobs for the next exclusive model start — necessary when the ISE cluster can only load one large model at a time. Models that do not match any exclusive prefix are treated as passthrough and run in parallel up to `--concurrency` without the serialization constraint.

When the grid sets `per_model_exclusive: true`, each distinct `model_name` is limited to one in-flight job at a time (in addition to the global `--concurrency` cap). Prefix-based exclusive grouping is not used in that mode — typical for sweeps where you want one worker per model (e.g. `concurrency: 3` with three models).

### Progress output

Each completed run prints a one-line summary:

```
[done/total] spec_name @ fixture repN: ok (42s)
```

`resume` also prints the number of skipped runs before the first new one.

### Live trace (tail + `--verbose`)

`--verbose` / `-v` mirrors each appended [`trace.jsonl`](data-formats.md#tracejsonl) line to stderr immediately. If you prefer to read the file, pin `--run-id` and tail it from another terminal:

```bash
tail -f aidmi_workspace/runs/<run-id>/trace.jsonl
```

```bash
tail -f aidmi_workspace/runs/<run-id>/trace.jsonl | jq -c '{ts:.timestamp, type:.event_type, label:(.label//.tool_name//.role)}'
```

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

expands into three cells, one per `context_mode` value. Each row records a distinct `strategy_spec_name` (derived from optional cell `name`, else from `strategy`, plus suffixes such as `_context_mode_metadata_only`). The expansion applies to top-level scalar fields only; nested fields like `writer_model.model_name` are not expanded. Named model refs in the `models:` block are an exception: a list of ref names in a `*_model` field expands cartesian-wise before resolving to ModelSpec dicts (see [Configuration](configuration.md#grid-yaml)).

### Output

```
<out>/
├── results.jsonl       # one BenchmarkResult per line, streamed as cells complete
└── sweep_config.yaml   # the grid that was run, for reproducibility
```

Each run's full per-cell artifacts (trace, dbt project, result.json) are also written under `<workspace>/runs/<run-id>/` so individual cells can be inspected offline.

### Resume semantics

`results.jsonl` is appended to, never replaced, when `--resume` is active. Completed-key lookup is based on `(strategy_spec_name, fixture_name, rep_index)` from existing rows. Interrupted sweeps resume from the first missing run without re-running completed cells.

### Failure handling

A cell that fails (strategy crash, dbt error, missing API key) does not stop the sweep. The cell's `BenchmarkResult` is written with `error` set to a string describing the failure, and the next cell starts. The `results.jsonl` always contains exactly one row per cell.

A `KeyboardInterrupt` or other signal stops the sweep at the next cell boundary. Already-completed cells remain in `results.jsonl`.

## `aidmi-orchestrator report`

Aggregate one or more sweep result directories into markdown and CSV summary tables and SVG strategy×model heatmaps.

```
aidmi-orchestrator report \
  PATH [PATH ...] \
  [--out DIR] \
  [--matrix-metric METRIC] \
  [--metrics METRIC,...] \
  [--no-plots]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `PATH ...` | One or more paths to sweep output directories (containing `results.jsonl`) or bare `results.jsonl` files. Multiple directories are merged before aggregation. |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out` | `./report` | Output directory for report files. Created if absent. |
| `--matrix-metric` | `target_columns_covered` | Metric used to build the strategy × model matrix table in `summary.md`. |
| `--metrics` | built-in list | Comma-separated list of headline metrics to display in the summary table. Overrides the default set. |
| `--no-plots` | off | Skip plot artifacts. By default, SVG plots and sibling CSVs are written under `plots/{fixture}/global/`, `plots/{fixture}/by_strategy/{strategy}/`, and `plots/{fixture}/pairs/`. Requires the `plots` extra (`just install` or `uv sync --extra plots`). |

### Output files

| File | Contents |
|------|---------|
| `summary.md` | Markdown table of headline metrics per cell (mean ± std), followed by a strategy × model matrix for `--matrix-metric`. |
| `cells.csv` | One row per `(fixture, spec, strategy, model, metric)` with `mean`, `std`, and `n`. |
| `summary.csv` | One row per cell `(fixture, spec, strategy, model)` with per-metric **means** as columns (wide format). |
| `plots/{fixture}/global/{metric}.svg` | Strategy × model heatmap for that metric. Omitted when `--no-plots` is given. |
| `plots/{fixture}/global/{metric}.csv` | Tidy data for the sibling SVG (`strategy`, `model`, `value`). Written together with the SVG. |
| `plots/{fixture}/by_strategy/{strategy}/tokens_*_by_role.svg` | Stacked bar of mean per-role breakdown (tokens, LLM calls, latency) for multi-role strategies. |
| `plots/{fixture}/by_strategy/{strategy}/rep_stability_{metric}.svg` | Box/strip plot of per-rep values by model (`dbt_success`, `preservation_row_ratio_mean`, `llm_calls_total`, `tokens_input_total`). |
| `plots/{fixture}/by_strategy/{strategy}/outcome_funnel.svg` | Grouped bar of pass rate per pipeline stage by model. Thresholds: `FUNNEL_TARGET_COLUMNS_MIN` (0.9), `FUNNEL_PRESERVATION_ROW_RATIO_MIN` (0.95) in `strategy_plots.py`. |
| `plots/{fixture}/by_strategy/{strategy}/preservation_profile.svg` | Grouped bar of row/distinct/null-inflation preservation metrics by model. |
| `plots/{fixture}/by_strategy/{strategy}/schema_errors.svg` | Grouped bar of `type_mismatches` and `extraneous_columns` by model. |
| `plots/{fixture}/by_strategy/{strategy}/tokens_in_out.svg` | Grouped bar of input vs output tokens by model. |
| `plots/{fixture}/by_strategy/{strategy}/preservation_per_table.svg` | Grouped bar of mean `row_ratio` per target table by model. |
| `plots/{fixture}/by_strategy/{strategy}/row_equality_per_table.svg` | Table × model heatmap of mean `row_count_match` (when reference equality data exists). |
| `plots/{fixture}/pairs/self_correction/{metric}.svg` | Dumbbell plot comparing `structured_per_table` vs `structured_per_table_sc` by model. |
| `plots/{fixture}/by_strategy/{strategy}/*.csv` | Tidy data for the sibling SVG in the same directory. |

## Examples

Run the mock strategy on the SP1 fixture (typically `just up` from repo root plus a `.env` matching [`.env.example`](../.env.example)):

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

Local Ollama benchmark (Postgres up, models pulled; campaign dir `benchmarks/2026-06-17-1/`):

```bash
just up
just sweep 2026-06-17-1 ollama_snapshot
just report 2026-06-17-1
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

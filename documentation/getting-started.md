# Getting started

This page walks through installing the workspace, running the bundled `sp1_users` fixture against the deterministic `mock` strategy, and reading the resulting artifacts. The `mock` strategy makes no LLM calls and is the recommended way to verify the harness on a new machine.

## Install

```bash
git clone git@github.com:tub-aidmi/aidmi.git
cd aidmi
nix develop                # optional: provides python 3.13, uv, podman
uv sync --all-packages
```

`uv sync --all-packages` is required (rather than plain `uv sync`) because `aidmi-orchestrator` is a workspace member package; without `--all-packages` only the root project's dependencies are installed.

## Verify

Run the deterministic test suite. This starts a Postgres container, runs the orchestrator end-to-end with the mock strategy, executes dbt twice (once for the generated mapping, once for the reference dbt project bundled with the fixture), and asserts the produced rows equal the reference.

```bash
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

Expected output ends with `37 passed`. Typical wall-clock is 50–60 seconds (Postgres container startup plus two dbt runs).

If the test fails to start the Postgres container with `Could not find Docker daemon`, set `DOCKER_HOST` to your Podman socket: `export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock`. The bundled `conftest.py` does this automatically when the socket file exists.

## Run the bundled demo via the CLI

The CLI requires a Postgres connection string. The simplest way to provide one is via `testcontainers` in a shell script, but for a one-off run you can use any Postgres instance:

```bash
# Start a temporary Postgres
podman run --rm -d --name aidmi-staging -p 5432:5432 \
  -e POSTGRES_PASSWORD=test postgres:16-alpine
export AIDMI_STAGING_DB_URL=postgresql://postgres:test@localhost:5432/postgres

uv run --package aidmi-orchestrator aidmi-orchestrator run \
  --fixture sp1_users \
  --strategy-spec packages/orchestrator/examples/strategy_specs/mock.yaml
```

The command prints a `BenchmarkResult` as JSON and writes artifacts under `./aidmi_workspace/runs/<ulid>/`.

Tear down the container when finished:

```bash
podman stop aidmi-staging
```

## What the run produced

```
aidmi_workspace/runs/<ulid>/
├── trace.jsonl                    # one event per line; see data-formats.md
├── dbt_project/
│   ├── dbt_project.yml
│   └── models/
│       ├── users.sql              # the SQL the strategy wrote
│       └── sources.yml
├── mapping_manifest.json          # per-column mapping notes (mock provided one)
├── strategy_result.json
└── result.json                    # BenchmarkResult with evaluator metrics
```

Key metric keys in `result.json`:

```json
{
  "metrics": {
    "dbt_success": true,
    "dbt_models_succeeded": 1,
    "row_count_match": true,
    "row_set_diff_count": 0,
    "target_columns_covered": 1.0,
    "produced_column_count": 7,
    "llm_calls_total": 0,
    "dollar_cost_total": 0.0
  }
}
```

`row_count_match=true` and `row_set_diff_count=0` mean the strategy's SQL produced output identical to the fixture's hand-written reference dbt project. `dollar_cost_total=0.0` because the mock strategy makes no LLM calls.

The full schema of `result.json` is documented in [data formats](data-formats.md).

## Run an LLM-driven strategy

```bash
export OPENAI_API_KEY=sk-...
export AIDMI_STAGING_DB_URL=postgresql://postgres:test@localhost:5432/postgres

uv run --package aidmi-orchestrator aidmi-orchestrator run \
  --fixture sp1_users \
  --strategy-spec packages/orchestrator/examples/strategy_specs/structured_per_table_openai.yaml
```

`metrics.dollar_cost_total` and `metrics.llm_calls_total` will be non-zero. Row equality is not guaranteed — the LLM may produce semantically equivalent SQL that differs in whitespace, column ordering, or case handling.

## Sweep multiple strategies

`aidmi-orchestrator sweep` runs a YAML-defined grid of `(strategy, config)` cells against one fixture and writes one `BenchmarkResult` per cell to a results JSONL.

```bash
uv run --package aidmi-orchestrator aidmi-orchestrator sweep \
  --grid packages/orchestrator/examples/day1_grid.yaml \
  --out aidmi_workspace/results/demo
```

The bundled `day1_grid.yaml` defines 6 cells: 1 mock + 3 `structured_per_table` (one per context mode) + 2 `write_tools_freeform` (with and without self-correction). LLM cells fail loudly when their API keys are missing; the mock cell always succeeds.

Read `aidmi_workspace/results/demo/results.jsonl` with pandas:

```python
import pandas as pd
df = pd.read_json("aidmi_workspace/results/demo/results.jsonl", lines=True)
df[["strategy_name", "metrics"]].head()
```

## Next steps

- [Concepts](concepts.md) explains what a strategy is and how the orchestrator drives it.
- [Configuration](configuration.md) is the reference for the YAML schemas used above.
- [Extending](extending.md) covers writing your own strategy, evaluator, fixture, or LLM provider.

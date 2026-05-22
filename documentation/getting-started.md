# Getting started

This page walks through installing the workspace, running the bundled `sp1_users` fixture against the deterministic `mock` strategy, and reading the resulting artifacts. The `mock` strategy makes no LLM calls and is the recommended way to verify the harness on a new machine.

## Prerequisites

Docker Desktop or Docker Engine (Compose v2 plugin) running on your machine—for the Postgres service in Compose and for `testcontainers` during pytest. With only rootless Podman and no Docker, tests may still work if pytest’s bundled socket detection finds a Docker-compatible Podman socket; that path is undocumented.

## Install

```bash
git clone git@github.com:tub-aidmi/aidmi.git
cd aidmi
nix develop                # optional: provides python 3.13, uv, docker-compose tooling
make env                   # cp .env.example → .env (first time only)
make install               # uv sync --all-packages
```

`uv sync --all-packages` is required (rather than plain `uv sync`) because `aidmi-orchestrator` is a workspace member package; without `--all-packages` only the root project's dependencies are installed.

## Verify

Run the deterministic test suite. This starts a Postgres container, runs the orchestrator end-to-end with the mock strategy, executes dbt twice (once for the generated mapping, once for the reference dbt project bundled with the fixture), and asserts the produced rows equal the reference.

You do **not** need `make up`; tests spin up Postgres via testcontainers instead.

```bash
make test
```

Or run one package directly:

```bash
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

Expected output: `make test` prints `2 passed` (pipeline) then `46 passed` (orchestrator). If you run only the orchestrator command above, you should see `46 passed`. Typical wall-clock is about 50–60 seconds (Postgres container startup plus two dbt runs).

## Run the bundled demo via the CLI

With a `.env` in the repo root, `aidmi-orchestrator` loads variables via `python-dotenv`. Staging Postgres is either `AIDMI_STAGING_DB_URL` or, if that is unset, a URL assembled from `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, and optionally `POSTGRES_HOST` / `POSTGRES_PORT` (see [`cli.md`](cli.md#environment)).

```bash
make up                    # Postgres on localhost (.env defaults in .env.example)
make demo                  # mock strategy; same as uv run aidmi-orchestrator run ...
```

The command prints a `BenchmarkResult` as JSON and writes artifacts under `./aidmi_workspace/runs/<ulid>/`. Add `-v` / `--verbose` to stream [`trace.jsonl`](data-formats.md#tracejsonl) lines to stderr as they are recorded, or pin `--run-id` and tail the same path from another shell (details in [`cli.md`](cli.md)).

Stop Postgres:


```bash
make down
```

Drop the volume as well (`make down-v`) if you want a clean data directory next time.

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

Set credentials in `.env` (see [`.env.example`](../.env.example)) or export them for the shell. Postgres from `make up` must match either the composed `POSTGRES_*` URL or `AIDMI_STAGING_DB_URL` if you override it.

```bash
make up

uv run --package aidmi-orchestrator aidmi-orchestrator run \
  --fixture sp1_users \
  --strategy-spec packages/orchestrator/examples/strategy_specs/structured_per_table_openai.yaml
```

`metrics.dollar_cost_total` and `metrics.llm_calls_total` will be non-zero. Row equality is not guaranteed — the LLM may produce semantically equivalent SQL that differs in whitespace, column ordering, or case handling.

## Sweep multiple strategies

`aidmi-orchestrator sweep` runs a YAML-defined grid of `(strategy, config)` cells against one fixture and writes one `BenchmarkResult` per cell to a results JSONL.

```bash
make up
make sweep
```

Equivalent:

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

## Salesforce → Pipedrive-shaped mapping (`sf_pipedrive`)

Phase 2 of the migration walkthrough: extract **Contact** and **Account** from Salesforce into staging, then drive an LLM strategy to emit dbt SQL for **persons** and **organizations** tables (staging only — no Pipedrive API upload).

Prerequisites:

- `.env`: Salesforce **`SF_USERNAME`**, **`SF_PASSWORD`**, and **`SF_SECURITY_TOKEN`** are all required (no **`SF_DOMAIN`**; SOAP uses `login.salesforce.com`) — see **[salesforce-auth.md](salesforce-auth.md)**.
- `LITELLM_API_KEY` when your strategy YAML sets `api_key_env: LITELLM_API_KEY`.
- Edit [`packages/orchestrator/examples/strategy_specs/write_tools_freeform_litellm_qwen.yaml`](../packages/orchestrator/examples/strategy_specs/write_tools_freeform_litellm_qwen.yaml): set `base_url` for your LiteLLM OpenAI-compatible endpoint (must include `/v1`).
- Postgres from `make up`.

**Check Salesforce credentials (no Postgres, no LLM):**

```bash
make sf-auth-check
```

This loads `.env`, performs SOAP login, and runs two tiny SOQL reads on Contact and Account.

Quick path:

```bash
make litellm-smoke-fixture     # LiteLLM + sp1_users (checks model wiring)
make up
make sf-pipedrive-litellm     # Salesforce extract → LLM dbt → run
```

Equivalent without Make:

```bash
make up

uv run --package aidmi-orchestrator aidmi-orchestrator run \
  --fixture sf_pipedrive \
  --strategy-spec packages/orchestrator/examples/strategy_specs/write_tools_freeform_litellm_qwen.yaml
```

Use [`structured_per_table_litellm_qwen.yaml`](../packages/orchestrator/examples/strategy_specs/structured_per_table_litellm_qwen.yaml) if your model reliably supports structured output; otherwise prefer `write_tools_freeform_*` as the primary path for Qwen-style models.

## Next steps

- [Concepts](concepts.md) explains what a strategy is and how the orchestrator drives it.
- [Configuration](configuration.md) is the reference for the YAML schemas used above.
- [Extending](extending.md) covers writing your own strategy, evaluator, fixture, or LLM provider.

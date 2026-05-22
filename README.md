# aidmi

Research platform for evaluating AI-driven data migration. Currently in early development at TU Berlin.

The platform decomposes into four sub-projects:

| # | Sub-project | Status |
|---|-------------|--------|
| 1 | Pipeline foundation — `dlt → Postgres staging → dbt → dlt` substrate (`packages/pipeline/`) | Implemented |
| 2 | AI orchestrator — benchmark harness for LLM-driven dbt mapping generation (`packages/orchestrator/`) | Implemented |
| 3 | HITL & state platform — FastAPI service wrapping the orchestrator with persistent state and real-time events | Pending |
| 4 | Frontend — Vite + React + shadcn UI (`frontend/`) | Pending |

## Documentation

User-facing documentation for the orchestrator (sub-project 2) lives in [`documentation/`](documentation/README.md). Start with [Getting started](documentation/getting-started.md).

## Dev setup

Requirements: Docker (Docker Desktop or Engine with Compose), Python 3.13, [`uv`](https://docs.astral.sh/uv/). Optional `nix develop` for Node, Compose binaries, Python, and uv in one shell ([`flake.nix`](flake.nix)).

Quick path:

```bash
make env                   # cp .env.example → .env (first time only)
make install               # uv sync --all-packages
make test                  # pytest for pipeline + orchestrator (uses testcontainers; no Postgres compose needed)

make up                    # Postgres for CLI runs via docker-compose.yml
make demo                  # bundled mock-strategy orchestrator demo
make down                  # tear down Postgres
```

The full target list (`make`, `make help`) includes `demo`, `sweep`, `psql`, `logs`, `down-v`, etc.

Equivalent without Make:

```bash
nix develop                                              # optional: node, compose tooling, python 3.13, uv
uv sync --all-packages                                   # install workspace + dev deps

# Sub-project 1 tests:
uv run --package aidmi-pipeline pytest packages/pipeline/tests/

# Sub-project 2 tests (deterministic suite):
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

Both test suites use `testcontainers` to spin up Postgres automatically. Prefer Docker daemon + Compose on the documented path (see [getting started](documentation/getting-started.md)). With rootless Podman only, pytest’s socket detection may still find a usable socket; Docker remains the baseline.

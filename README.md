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

Requirements: Docker (Docker Desktop or Engine with Compose), Python 3.13, [`uv`](https://docs.astral.sh/uv/), [`just`](https://github.com/casey/just) (`brew install just`). Optional `nix develop` for Node, Compose binaries, Python, and uv in one shell ([`flake.nix`](flake.nix)).

Quick path:

```bash
just env                   # cp .env.example → .env (first time only)
just install               # uv sync --all-packages
just test                  # pytest for pipeline + orchestrator (uses testcontainers; no Postgres compose needed)

just up                    # Postgres for CLI runs via docker-compose.yml
just demo                  # bundled mock-strategy orchestrator demo
just down                  # tear down Postgres
```

Run `just` or `just --list` for all recipes (`sweep`, `report`, `psql`, `logs`, `down-v`, etc.).

Equivalent without Just:

```bash
nix develop                                              # optional: node, compose tooling, python 3.13, uv
uv sync --all-packages                                   # install workspace + dev deps

# Sub-project 1 tests:
uv run --package aidmi-pipeline pytest packages/pipeline/tests/

# Sub-project 2 tests (deterministic suite):
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

Both test suites use `testcontainers` to spin up Postgres automatically. Prefer Docker daemon + Compose on the documented path (see [getting started](documentation/getting-started.md)). With rootless Podman only, pytest’s socket detection may still find a usable socket; Docker remains the baseline.

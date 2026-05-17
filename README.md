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

```bash
nix develop                                              # provides node, podman, python 3.13, uv
uv sync --all-packages                                   # install workspace + dev deps

# Sub-project 1 tests:
uv run --package aidmi-pipeline pytest packages/pipeline/tests/

# Sub-project 2 tests (deterministic suite):
uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"
```

Both test suites use `testcontainers` to spin up Postgres. Docker or rootless Podman is required; the test harness auto-detects the Podman socket at `/run/user/$UID/podman/podman.sock`.

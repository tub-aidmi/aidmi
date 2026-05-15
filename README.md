# aidmi — AI-Driven Data Migration

Multi-tenant data migration platform. Currently in early prototype.

## Status

**Sub-project 1 (pipeline foundation):** implemented. End-to-end `dlt → Postgres → dbt → dlt` pipeline with parameterized seams. See `packages/pipeline/`.

**Coming next:** AI orchestrator (PydanticAI), HITL state platform, frontend.

## Dev setup

```bash
nix develop                                              # provides nodejs, docker-compose/podman-compose, python3+uv
uv sync                                                  # install workspace + dev deps
uv run pytest packages/pipeline/tests/ -v                # run tests (starts a Postgres container)
```

## Architecture (sub-project 1 only)

dlt extracts JSONL → loads into staging Postgres dataset → `dlt.dbt.package()` runs dbt transform in-place → dlt loads transformed table to filesystem JSONL destination. The pipeline package accepts opaque `dlt` source/destination objects so future callers can plug in real CRM sources without changing this package.

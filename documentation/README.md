# aidmi orchestrator — documentation

The orchestrator (`packages/orchestrator/`) generates dbt SQL mappings between a source database and a target schema using an LLM-driven strategy, runs the generated dbt against a Postgres staging dataset, and scores the output with a set of evaluators. The same harness runs multiple strategies against the same fixture for direct comparison.

The tool is intended for research: comparing prompts, agent topologies, models, and context-exposure levels against each other on output quality, token cost, and latency.

## Contents

| Page | Audience |
|------|----------|
| [Getting started](getting-started.md) | First-time setup, run the bundled demo. |
| [Concepts](concepts.md) | What an orchestrator, strategy, evaluator, and fixture are, and how they fit together. |
| [CLI reference](cli.md) | `aidmi-orchestrator run` and `sweep` commands. |
| [Configuration](configuration.md) | Strategy-spec, grid, and pricing-override YAML schemas. |
| [Data formats](data-formats.md) | The on-disk artifacts a run produces. |
| [Extending](extending.md) | Writing new strategies, evaluators, fixtures, and LLM providers. |
| [Architecture](architecture.md) | Internal package layout, control flow, registries. |
| [Salesforce SOAP auth (`sf_pipedrive`)](salesforce-auth.md) | `SF_USERNAME` / password / token, Trusted IPs, SOAP login troubleshooting. |

## Scope

The orchestrator stops at the *generation* boundary: it produces a validated dbt project and a `BenchmarkResult`, but it does not push data to a production destination. Production reverse-ETL is the responsibility of sub-project 3, which wraps the orchestrator with a state machine and HTTP API.

The orchestrator requires a running Postgres instance for staging. In tests this is provided by `testcontainers`; for CLI runs configure `AIDMI_STAGING_DB_URL`, or omit it and rely on `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` (optional host/port)—see [`cli.md`](cli.md#environment)—typically from [`.env.example`](../.env.example) copied to `.env`; the CLI loads `.env` automatically.

## Prerequisites

- Python 3.13
- `uv` (workspace package manager)
- Docker Desktop or Docker Engine with Compose—for local staging Postgres (`make up`), and for Postgres started by pytest via `testcontainers`. Rootless Podman may happen to work with pytest’s bundled socket detection; Docker is assumed in the docs.
- One or more LLM API keys (OpenAI, Anthropic, or a local Ollama server) for the LLM-driven strategies. The bundled `mock` strategy requires no LLM and is useful for harness validation.

See [Getting started](getting-started.md) for the install procedure.

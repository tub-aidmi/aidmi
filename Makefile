COMPOSE ?= docker compose
POSTGRES_USER ?= postgres
POSTGRES_DB ?= postgres

-include .env
export

.DEFAULT_GOAL := help

.PHONY: help env install setup up down down-v logs psql test test-orchestrator test-pipeline demo sweep clean-workspace litellm-smoke-fixture sf-pipedrive-litellm

help: ## List targets (make test works without Postgres)
	@echo "Makefile targets:"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"} { printf "  %-20s %s\n", $$1, $$2 }'

env: ## Create .env from .env.example if missing
	cp -n .env.example .env

install: ## Install workspace deps (uv sync --all-packages)
	uv sync --all-packages

setup: install ## Alias for install

up: ## Start Postgres staging (docker compose up -d --wait)
	$(COMPOSE) up -d --wait

down: ## Stop Postgres and remove containers
	$(COMPOSE) down

down-v: ## Stop Postgres and delete the named volume
	$(COMPOSE) down -v

logs: ## Follow Postgres logs
	$(COMPOSE) logs -f postgres

psql: ## Interactive psql in the Postgres container
	$(COMPOSE) exec postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)"

test-pipeline: ## Run pipeline package tests
	uv run --package aidmi-pipeline pytest packages/pipeline/tests/

test-orchestrator: ## Run orchestrator tests (excluding LLM)
	uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"

test: test-pipeline test-orchestrator ## Run both test suites (no make up needed)

demo: ## Mock orchestrator run (needs: make env, make up)
	@test -f .env || cp -n .env.example .env
	uv run --package aidmi-orchestrator aidmi-orchestrator run \
		--fixture sp1_users \
		--strategy-spec packages/orchestrator/examples/strategy_specs/mock.yaml

litellm-smoke-fixture: ## LiteLLM + bundled sp1_users (needs: make env, make up, LITELLM base_url edited)
	@test -f .env || cp -n .env.example .env
	uv run --package aidmi-orchestrator aidmi-orchestrator run \
		--fixture sp1_users \
		--strategy-spec packages/orchestrator/examples/strategy_specs/write_tools_freeform_litellm_qwen.yaml

sf-pipedrive-litellm: ## SF Contact+Account → Pipedrive-shaped dbt (needs: make env, make up, SF_* + LiteLLM)
	@test -f .env || cp -n .env.example .env
	uv run --package aidmi-orchestrator aidmi-orchestrator run \
		--fixture sf_pipedrive \
		--strategy-spec packages/orchestrator/examples/strategy_specs/write_tools_freeform_litellm_qwen.yaml

sweep: ## Run demo sweep grid (needs Postgres + LLM keys for LLM cells)
	@test -f .env || cp -n .env.example .env
	uv run --package aidmi-orchestrator aidmi-orchestrator sweep \
		--grid packages/orchestrator/examples/day1_grid.yaml \
		--out aidmi_workspace/results/demo

clean-workspace: ## Remove ./aidmi_workspace
	rm -rf aidmi_workspace

set dotenv-load

compose := "docker compose"
orch := "uv run --package aidmi-orchestrator aidmi-orchestrator"
orch-py := "uv run --package aidmi-orchestrator"
orch-test := "uv run --extra plots --package aidmi-orchestrator"
specs := "packages/orchestrator/examples/strategy_specs"
benchmarks := "benchmarks"

default:
  @just --list

# --- Dev environment ---

env:
  cp -n .env.example .env

install:
  uv sync --all-packages --extra plots

setup: install

# --- Postgres ---

up:
  {{compose}} up -d --wait

down:
  {{compose}} down

down-v:
  {{compose}} down -v

logs:
  {{compose}} logs -f postgres

psql:
  {{compose}} exec postgres psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}"

# --- Fixtures ---

init-db *FIXTURES:
  @test -f .env || cp -n .env.example .env
  {{orch-py}} python -m aidmi_orchestrator.scripts.init_fixtures {{FIXTURES}}

gen-target-schema fixture:
  {{orch-py}} python -m aidmi_orchestrator.scripts.gen_target_schema --fixture {{fixture}}

gen-target-schema-file input output:
  {{orch-py}} python -m aidmi_orchestrator.scripts.gen_target_schema --input {{input}} --output {{output}}

# --- Tests ---

test-pipeline:
  uv run --package aidmi-pipeline pytest packages/pipeline/tests/

test-orchestrator:
  {{orch-test}} pytest packages/orchestrator/tests/ -m "not requires_llm"

test: test-pipeline test-orchestrator

# --- Campaigns ---

campaign-new label="":
  {{orch}} campaign new {{label}}

campaign-use id:
  {{orch}} campaign use {{id}}

campaign:
  {{orch}} campaign show

# --- Orchestrator runs (always recorded to active campaign) ---

run fixture spec:
  @test -f .env || cp -n .env.example .env
  {{orch}} run --fixture {{fixture}} --strategy-spec {{specs}}/{{spec}}.yaml

demo:
  just campaign-new demo
  just init-db mock
  just run mock mock

litellm-smoke:
  just campaign-new litellm-smoke
  just init-db master
  just run master write_tools_freeform_litellm_qwen

ollama-smoke:
  just campaign-new ollama-smoke
  just init-db master
  just run master write_tools_freeform_ollama_qwen

# --- Sweeps (uses campaign's grid.yaml) ---

sweep campaign="":
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  if [ -n "{{campaign}}" ]; then
    {{orch}} sweep --campaign "{{benchmarks}}/{{campaign}}"
  else
    {{orch}} sweep
  fi

sweep-verbose campaign="":
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  if [ -n "{{campaign}}" ]; then
    {{orch}} sweep --campaign "{{benchmarks}}/{{campaign}}" -v
  else
    {{orch}} sweep -v
  fi

report campaign="":
  #!/usr/bin/env bash
  set -euo pipefail
  if [ -n "{{campaign}}" ]; then
    target="{{benchmarks}}/{{campaign}}"
  else
    target="$({{orch}} campaign show | cut -f1)"
    target="{{benchmarks}}/$target"
  fi
  {{orch}} report "$target" --out "$target/report"

sweep-demo:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  just campaign-new sweep-demo
  camp="$({{orch}} campaign show | cut -f2)"
  cp packages/orchestrator/examples/day1_grid.yaml "$camp/grid.yaml"
  {{orch}} sweep

# --- dbt repro ---

apply-dbt run_id campaign="":
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  if [ -n "{{campaign}}" ]; then
    {{orch}} apply-dbt --run-id {{run_id}} --campaign "{{benchmarks}}/{{campaign}}"
  else
    {{orch}} apply-dbt --run-id {{run_id}}
  fi

repro run_id campaign="":
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  if [ -n "{{campaign}}" ]; then
    camp="{{benchmarks}}/{{campaign}}"
    extra=(--campaign "$camp")
  else
    camp="$({{orch}} campaign show | cut -f2)"
    extra=()
  fi
  result_json="$camp/runs/{{run_id}}/result.json"
  if [ -f "$result_json" ]; then
    fixture=$(jq -r .fixture_name "$result_json")
  elif [ -f "$camp/results.jsonl" ]; then
    fixture=$(jq -r --arg id "{{run_id}}" 'select(.run_id==$id) | .fixture_name' "$camp/results.jsonl" | head -1)
  elif [ -f "$camp/results/results.jsonl" ]; then
    fixture=$(jq -r --arg id "{{run_id}}" 'select(.run_id==$id) | .fixture_name' "$camp/results/results.jsonl" | head -1)
  else
    echo "no result found for run {{run_id}} under $camp" >&2
    exit 1
  fi
  if [ -z "$fixture" ] || [ "$fixture" = "null" ]; then
    echo "could not resolve fixture for run {{run_id}}" >&2
    exit 1
  fi
  just init-db "$fixture"
  {{orch}} apply-dbt --run-id {{run_id}} "${extra[@]}"
  {{orch}} evaluate --run-id {{run_id}} "${extra[@]}"

clean-workspace:
  rm -rf aidmi_workspace

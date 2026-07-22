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

build-fixtures:
  {{orch-py}} python -m aidmi_orchestrator.scripts.build_fixtures

# --- Quality ---

lint:
  uv run ruff check .
  uv run ruff format --check .

format:
  uv run ruff format .

test-pipeline:
  uv run --package aidmi-pipeline pytest packages/pipeline/tests/

test-orchestrator:
  {{orch-test}} pytest packages/orchestrator/tests/ -m "not requires_llm"

test: test-pipeline test-orchestrator

verify-results:
  {{orch-test}} python -m aidmi_orchestrator.scripts.verify_results

snapshot-results:
  {{orch-test}} python -m aidmi_orchestrator.scripts.verify_results --snapshot

verify-fixtures *ARGS:
  {{orch-py}} python -m aidmi_orchestrator.scripts.verify_fixtures {{ARGS}}

# --- Campaigns ---

campaign-new label="":
  {{orch}} campaign new {{label}}

# --- Orchestrator runs ---

run campaign fixture spec:
  @test -f .env || cp -n .env.example .env
  {{orch}} run --campaign "{{benchmarks}}/{{campaign}}" --fixture {{fixture}} --strategy-spec {{specs}}/{{spec}}.yaml

demo:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  camp_id=$({{orch}} campaign new demo | awk '{print $3}')
  just init-db mock
  {{orch}} run --campaign "{{benchmarks}}/$camp_id" --fixture mock --strategy-spec {{specs}}/mock.yaml

litellm-smoke:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  camp_id=$({{orch}} campaign new litellm-smoke | awk '{print $3}')
  just init-db master
  {{orch}} run --campaign "{{benchmarks}}/$camp_id" --fixture master --strategy-spec {{specs}}/write_tools_freeform_litellm_qwen.yaml

ollama-smoke:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  camp_id=$({{orch}} campaign new ollama-smoke | awk '{print $3}')
  just init-db master
  {{orch}} run --campaign "{{benchmarks}}/$camp_id" --fixture master --strategy-spec {{specs}}/write_tools_freeform_ollama_qwen.yaml

# --- Sweeps ---

sweep campaign:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  {{orch}} sweep --campaign "{{benchmarks}}/{{campaign}}"

sweep-verbose campaign:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  {{orch}} sweep --campaign "{{benchmarks}}/{{campaign}}" -v

report campaign:
  {{orch}} report "{{benchmarks}}/{{campaign}}" --out "{{benchmarks}}/{{campaign}}/report"

sweep-demo:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  camp_id=$({{orch}} campaign new sweep-demo | awk '{print $3}')
  cp packages/orchestrator/examples/day1_grid.yaml "{{benchmarks}}/$camp_id/grid.yaml"
  {{orch}} sweep --campaign "{{benchmarks}}/$camp_id"

# --- dbt repro ---

apply-dbt run_id campaign:
  @test -f .env || cp -n .env.example .env
  {{orch}} apply-dbt --run-id {{run_id}} --campaign "{{benchmarks}}/{{campaign}}"

repro run_id campaign:
  #!/usr/bin/env bash
  set -euo pipefail
  test -f .env || cp -n .env.example .env
  camp="{{benchmarks}}/{{campaign}}"
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
  {{orch}} apply-dbt --run-id {{run_id}} --campaign "$camp"
  {{orch}} evaluate --run-id {{run_id}} --campaign "$camp"

clean-workspace:
  rm -rf aidmi_workspace

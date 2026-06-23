set dotenv-load

compose := "docker compose"
orch := "uv run --package aidmi-orchestrator aidmi-orchestrator"
orch-py := "uv run --package aidmi-orchestrator"
specs := "packages/orchestrator/examples/strategy_specs"

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
  uv run --package aidmi-orchestrator pytest packages/orchestrator/tests/ -m "not requires_llm"

test: test-pipeline test-orchestrator

# --- Orchestrator single runs ---

run fixture spec:
  @test -f .env || cp -n .env.example .env
  {{orch}} run --fixture {{fixture}} --strategy-spec {{specs}}/{{spec}}.yaml

demo:
  just init-db mock
  just run mock mock

litellm-smoke:
  just init-db master
  just run master write_tools_freeform_litellm_qwen

ollama-smoke:
  just init-db master
  just run master write_tools_freeform_ollama_qwen

# --- Benchmarks ---
# Historic campaign results: benchmarks/historic/<campaign>/
# Usage: just sweep historic/2026-06-17-1 ollama_snapshot
#        just sweep 08-06-2026 main_grid results/main
#        just archive-dbt 2026-06-17-1   # backfill dbt from workspace into results/dbt/

# Pass flags only after naming out=, e.g. just sweep 2026-06-23 x out=results -v
# Or use sweep-verbose (bare -v is parsed as the out path).
sweep campaign grid out="results" *FLAGS:
  @test -f .env || cp -n .env.example .env
  {{orch}} sweep \
    --grid benchmarks/{{campaign}}/grids/{{grid}}.yaml \
    --out benchmarks/{{campaign}}/{{out}} \
    {{FLAGS}}

sweep-verbose campaign grid out="results":
  @test -f .env || cp -n .env.example .env
  {{orch}} sweep \
    --grid benchmarks/{{campaign}}/grids/{{grid}}.yaml \
    --out benchmarks/{{campaign}}/{{out}} \
    -v

archive-dbt campaign out="results":
  {{orch}} archive-dbt --out benchmarks/{{campaign}}/{{out}}

report campaign *inputs:
  #!/usr/bin/env bash
  set -euo pipefail
  campaign="{{campaign}}"
  if [ -n "{{inputs}}" ]; then
    # shellcheck disable=SC2206
    dirs=({{inputs}})
  else
    dirs=()
    while IFS= read -r f; do
      d=$(dirname "$f")
      case " ${dirs[*]:-} " in
        *" $d "*) ;;
        *) dirs+=("$d") ;;
      esac
    done < <(find "benchmarks/$campaign" -name results.jsonl 2>/dev/null)
    if [ "${#dirs[@]}" -eq 0 ]; then
      echo "no results.jsonl found under benchmarks/$campaign" >&2
      exit 1
    fi
  fi
  uv run --extra plots --package aidmi-orchestrator aidmi-orchestrator report \
    "${dirs[@]}" --out "benchmarks/$campaign/report"

sweep-demo:
  @test -f .env || cp -n .env.example .env
  {{orch}} sweep \
    --grid packages/orchestrator/examples/day1_grid.yaml \
    --out aidmi_workspace/results/demo

clean-workspace:
  rm -rf aidmi_workspace

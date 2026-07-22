"""Snapshot per-token pricing from the LiteLLM proxy into configs/pricing.json.

The ISE LiteLLM proxy exposes per-model `input_cost_per_token` /
`output_cost_per_token` via its `/model/info` endpoint (context windows and
reasoning-token breakdowns are not reported). LiteLLM's bundled `model_cost`
table does not know the ISE custom model names, so `llm_usage` cannot price
those runs without an override. This script fetches the proxy's own prices and
writes them keyed as `<provider>/<model_name>` so the evaluator can compute
`dollar_cost_total` for ISE benchmark runs.

Usage (with the SSH tunnel up and LITELLM_API_KEY in the environment):

    uv run --package aidmi-orchestrator python \
        packages/orchestrator/scripts/fetch_ise_pricing.py

By default it targets http://localhost:4000 and emits keys for provider
`litellm` (what the ISE grids use). Override with --base-url / --provider.
"""

from __future__ import annotations
import argparse
import json
import os
import urllib.request
from pathlib import Path

from aidmi_orchestrator.pricing import default_pricing_config_path


def fetch_model_info(base_url: str, api_key: str) -> list[dict]:
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/model/info",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    data = payload.get("data", payload)
    if not isinstance(data, list):
        raise SystemExit(f"unexpected /model/info payload: {type(data)}")
    return data


def build_overrides(models: list[dict], providers: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for entry in models:
        name = (entry.get("model_name") or "").strip()
        info = entry.get("model_info", {})
        in_cost = info.get("input_cost_per_token")
        out_cost = info.get("output_cost_per_token")
        if not name or name == "ignored" or in_cost is None or out_cost is None:
            continue
        if info.get("mode") == "embedding":
            continue
        record: dict[str, float] = {
            "input_cost_per_token": float(in_cost),
            "output_cost_per_token": float(out_cost),
        }
        cached = info.get("cache_read_input_token_cost")
        if cached is not None:
            record["cached_input_cost_per_token"] = float(cached)
        for provider in providers:
            out[f"{provider}/{name}"] = record
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:4000")
    parser.add_argument("--api-key-env", default="LITELLM_API_KEY")
    parser.add_argument(
        "--provider",
        action="append",
        dest="providers",
        help="provider prefix for override keys (repeatable); default: litellm",
    )
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise SystemExit(f"{args.api_key_env} not set in environment")
    providers = args.providers or ["litellm"]
    out_path = args.out or default_pricing_config_path()

    models = fetch_model_info(args.base_url, api_key)
    overrides = build_overrides(models, providers)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(overrides, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(overrides)} entries to {out_path}")


if __name__ == "__main__":
    main()

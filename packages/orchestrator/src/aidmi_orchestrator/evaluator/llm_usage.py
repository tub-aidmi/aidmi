"""LlmUsageEvaluator — token counts, cache rate, dollar cost (LiteLLM + override)."""
from __future__ import annotations
import statistics
from collections import defaultdict
from typing import Any

from aidmi_orchestrator.evaluator.base import (
    Evaluator, RunArtifacts, register_evaluator,
)
from aidmi_orchestrator.trace import LlmCallEvent
from aidmi_orchestrator.pricing import lookup_price, load_overrides


def _extract_cached_input(usage: dict) -> int:
    # R8 spike: PydanticAI normalizes cache tokens from all providers into cache_read_tokens
    # before TracedModel sees them — no need for provider-specific parsing.
    return int(usage.get("cache_read_tokens", 0) or 0)


class LlmUsageEvaluator:
    name = "llm_usage"

    def __init__(self, pricing_override_path=None):
        self._overrides = load_overrides(pricing_override_path)

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return any(isinstance(e, LlmCallEvent) for e in artifacts.trace)

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        calls_by_role: dict[str, int] = defaultdict(int)
        tokens_in_total = 0
        tokens_in_cached = 0
        tokens_out_total = 0
        cost_total = 0.0
        cost_by_role: dict[str, float] = defaultdict(float)
        latency_by_role: dict[str, list[float]] = defaultdict(list)

        for ev in artifacts.trace:
            if not isinstance(ev, LlmCallEvent):
                continue
            usage = ev.usage or {}
            prompt = int(usage.get("input_tokens", 0) or 0)
            completion = int(usage.get("output_tokens", 0) or 0)
            cached = _extract_cached_input(usage)
            uncached = max(0, prompt - cached)

            calls_by_role[ev.role] += 1
            tokens_in_total += prompt
            tokens_in_cached += cached
            tokens_out_total += completion
            latency_by_role[ev.role].append(ev.latency_ms)

            price = lookup_price(ev.model_spec.provider, ev.model_spec.model_name, self._overrides)
            if price is not None:
                cached_price = price.cached_input_cost_per_token or price.input_cost_per_token
                this_cost = (
                    uncached * price.input_cost_per_token
                    + cached * cached_price
                    + completion * price.output_cost_per_token
                )
                cost_total += this_cost
                cost_by_role[ev.role] += this_cost

        def _p50(vals: list[float]) -> float:
            return statistics.median(vals)

        def _p95(vals: list[float]) -> float:
            if len(vals) < 2:
                return vals[0]
            sorted_vals = sorted(vals)
            idx = int(len(sorted_vals) * 0.95)
            return sorted_vals[min(idx, len(sorted_vals) - 1)]

        latency_ms_total = sum(sum(v) for v in latency_by_role.values())

        return {
            "llm_calls_total": sum(calls_by_role.values()),
            "llm_calls_by_role": dict(calls_by_role),
            "tokens_input_total": tokens_in_total,
            "tokens_input_cached": tokens_in_cached,
            "tokens_input_uncached": tokens_in_total - tokens_in_cached,
            "tokens_output_total": tokens_out_total,
            "cache_hit_rate": (tokens_in_cached / tokens_in_total) if tokens_in_total else 0.0,
            "dollar_cost_total": cost_total,
            "dollar_cost_by_role": dict(cost_by_role),
            "latency_ms_by_role": {r: sum(v) / len(v) for r, v in latency_by_role.items()},
            "latency_ms_total": latency_ms_total,
            "latency_ms_p50_by_role": {r: _p50(v) for r, v in latency_by_role.items()},
            "latency_ms_p95_by_role": {r: _p95(v) for r, v in latency_by_role.items()},
        }


register_evaluator("llm_usage", LlmUsageEvaluator)

"""LlmUsageEvaluator — token counts, cache rate, dollar cost (LiteLLM + override)."""
from __future__ import annotations
import statistics
from collections import defaultdict
from typing import Any

from aidmi_orchestrator.evaluator.base import (
    Evaluator, RunArtifacts, register_evaluator,
)
from aidmi_orchestrator.trace import LlmCallEvent
from aidmi_orchestrator.pricing import lookup_price, lookup_context_limit, load_overrides


def _safe_int(value: Any) -> int:
    try:
        if value is None or isinstance(value, bool):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _extract_cached_input(usage: dict) -> int:
    return _safe_int(usage.get("cache_read_tokens", 0))


def _usage_details(usage: dict) -> dict[str, Any]:
    raw = usage.get("details")
    return raw if isinstance(raw, dict) else {}


def _detail_int(usage: dict, key: str) -> int:
    return _safe_int(_usage_details(usage).get(key, 0))


def _numeric_details(usage: dict) -> dict[str, int | float]:
    details = _usage_details(usage)
    out: dict[str, int | float] = {}
    for key, value in details.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out[str(key)] = value
    return out


def _vendor_str(usage: dict, key: str) -> str | None:
    vendor = usage.get("vendor")
    if not isinstance(vendor, dict):
        return None
    value = vendor.get(key)
    return value if isinstance(value, str) and value else None


class LlmUsageEvaluator:
    name = "llm_usage"

    def __init__(self, pricing_override_path=None):
        self._overrides = load_overrides(pricing_override_path)

    def applies_to(self, artifacts: RunArtifacts) -> bool:
        return any(isinstance(e, LlmCallEvent) for e in artifacts.trace)

    def evaluate(self, artifacts: RunArtifacts) -> dict[str, Any]:
        calls_by_role: dict[str, int] = defaultdict(int)
        tokens_in_by_role: dict[str, int] = defaultdict(int)
        tokens_out_by_role: dict[str, int] = defaultdict(int)
        tokens_thoughts_by_role: dict[str, int] = defaultdict(int)
        tokens_tool_use_prompt_by_role: dict[str, int] = defaultdict(int)
        tokens_input_peak_by_role: dict[str, int] = defaultdict(int)
        tokens_in_total = 0
        tokens_in_cached = 0
        tokens_out_total = 0
        tokens_thoughts_total = 0
        tokens_tool_use_prompt_total = 0
        tokens_input_peak = 0
        context_utilization_peak = 0.0
        llm_retries_total = 0
        usage_details_total: dict[str, float] = defaultdict(float)
        traffic_type_counts: dict[str, int] = defaultdict(int)
        cost_total = 0.0
        cost_by_role: dict[str, float] = defaultdict(float)
        latency_by_role: dict[str, list[float]] = defaultdict(list)
        latency_sum_by_role: dict[str, float] = defaultdict(float)

        for ev in artifacts.trace:
            if not isinstance(ev, LlmCallEvent):
                continue
            usage = ev.usage or {}
            prompt = _safe_int(usage.get("input_tokens", 0))
            completion = _safe_int(usage.get("output_tokens", 0))
            cached = _extract_cached_input(usage)
            uncached = max(0, prompt - cached)
            thoughts = _detail_int(usage, "thoughts_tokens")
            tool_use_prompt = _detail_int(usage, "tool_use_prompt_tokens")

            calls_by_role[ev.role] += 1
            tokens_in_by_role[ev.role] += prompt
            tokens_out_by_role[ev.role] += completion
            tokens_thoughts_by_role[ev.role] += thoughts
            tokens_tool_use_prompt_by_role[ev.role] += tool_use_prompt
            tokens_input_peak_by_role[ev.role] = max(tokens_input_peak_by_role[ev.role], prompt)
            tokens_in_total += prompt
            tokens_in_cached += cached
            tokens_out_total += completion
            tokens_thoughts_total += thoughts
            tokens_tool_use_prompt_total += tool_use_prompt
            tokens_input_peak = max(tokens_input_peak, prompt)
            latency_by_role[ev.role].append(ev.latency_ms)
            latency_sum_by_role[ev.role] += ev.latency_ms

            for key, value in _numeric_details(usage).items():
                usage_details_total[key] += float(value)

            traffic_type = _vendor_str(usage, "traffic_type")
            if traffic_type:
                traffic_type_counts[traffic_type] += 1

            llm_retries_total += _safe_int(usage.get("retry_count", 0))

            context_limit = lookup_context_limit(
                ev.model_spec.provider, ev.model_spec.model_name,
            )
            if context_limit and context_limit > 0:
                context_utilization_peak = max(
                    context_utilization_peak, prompt / context_limit,
                )

            price = lookup_price(ev.model_spec.provider, ev.model_spec.model_name, self._overrides)
            if price is not None:
                cached_price = price.cached_input_cost_per_token or price.input_cost_per_token
                this_cost = (
                    uncached * price.input_cost_per_token
                    + cached * cached_price
                    + completion * price.output_cost_per_token
                )
                if thoughts > 0:
                    reasoning_rate = (
                        price.reasoning_cost_per_token
                        or price.output_cost_per_token
                    )
                    if reasoning_rate:
                        this_cost += thoughts * reasoning_rate
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
            "tokens_input_by_role": dict(tokens_in_by_role),
            "tokens_input_cached": tokens_in_cached,
            "tokens_input_uncached": tokens_in_total - tokens_in_cached,
            "tokens_input_peak": tokens_input_peak,
            "tokens_input_peak_by_role": dict(tokens_input_peak_by_role),
            "tokens_output_total": tokens_out_total,
            "tokens_output_by_role": dict(tokens_out_by_role),
            "tokens_thoughts_total": tokens_thoughts_total,
            "tokens_thoughts_by_role": dict(tokens_thoughts_by_role),
            "tokens_tool_use_prompt_total": tokens_tool_use_prompt_total,
            "tokens_tool_use_prompt_by_role": dict(tokens_tool_use_prompt_by_role),
            "context_utilization_peak": context_utilization_peak,
            "llm_retries_total": llm_retries_total,
            "usage_details_total": dict(usage_details_total),
            "traffic_type_counts": dict(traffic_type_counts),
            "cache_hit_rate": (tokens_in_cached / tokens_in_total) if tokens_in_total else 0.0,
            "dollar_cost_total": cost_total,
            "dollar_cost_by_role": dict(cost_by_role),
            "latency_ms_by_role": {r: sum(v) / len(v) for r, v in latency_by_role.items()},
            "latency_ms_sum_by_role": dict(latency_sum_by_role),
            "latency_ms_total": latency_ms_total,
            "latency_ms_p50_by_role": {r: _p50(v) for r, v in latency_by_role.items()},
            "latency_ms_p95_by_role": {r: _p95(v) for r, v in latency_by_role.items()},
        }


register_evaluator("llm_usage", LlmUsageEvaluator)

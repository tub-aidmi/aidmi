"""Pricing lookup. LiteLLM's model_cost table is the default source; a JSON
override file can shadow or add entries.

Override file format: a single JSON object mapping `"provider/model_name"` to
`{"input_cost_per_token": float, "output_cost_per_token": float,
"cached_input_cost_per_token": float | None}`.
"""

from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PriceInfo:
    input_cost_per_token: float
    output_cost_per_token: float
    cached_input_cost_per_token: float | None = None
    max_input_tokens: int | None = None
    reasoning_cost_per_token: float | None = None


def default_pricing_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "configs" / "pricing.json"


def load_overrides(path: Path | None) -> dict[str, PriceInfo]:
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        key: PriceInfo(
            input_cost_per_token=v["input_cost_per_token"],
            output_cost_per_token=v["output_cost_per_token"],
            cached_input_cost_per_token=v.get("cached_input_cost_per_token"),
        )
        for key, v in raw.items()
    }


def _load_litellm_table() -> dict:
    try:
        import litellm

        return getattr(litellm, "model_cost", {})
    except ImportError:
        pass
    import importlib.util

    spec = importlib.util.find_spec("litellm")
    if spec is None or spec.origin is None:
        return {}
    pkg_dir = Path(spec.origin).parent
    for candidate in [
        "model_prices_and_context_window.json",
        "model_prices_and_context_window_backup.json",
    ]:
        p = pkg_dir / candidate
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {}


def _litellm_entry(provider: str, model_name: str) -> dict | None:
    table = _load_litellm_table()
    if not table:
        return None
    for key in (model_name, f"{provider}/{model_name}"):
        if key in table:
            return table[key]
    return None


def _price_from_entry(entry: dict) -> PriceInfo:
    max_input = entry.get("max_input_tokens")
    reasoning = entry.get("output_cost_per_reasoning_token")
    return PriceInfo(
        input_cost_per_token=float(entry.get("input_cost_per_token", 0.0)),
        output_cost_per_token=float(entry.get("output_cost_per_token", 0.0)),
        cached_input_cost_per_token=(
            float(entry["cache_read_input_token_cost"])
            if "cache_read_input_token_cost" in entry
            else None
        ),
        max_input_tokens=int(max_input) if max_input is not None else None,
        reasoning_cost_per_token=float(reasoning) if reasoning is not None else None,
    )


def _from_litellm(provider: str, model_name: str) -> PriceInfo | None:
    entry = _litellm_entry(provider, model_name)
    if entry is None:
        return None
    return _price_from_entry(entry)


def lookup_price(
    provider: str,
    model_name: str,
    overrides: dict[str, PriceInfo] | None = None,
) -> PriceInfo | None:
    overrides = overrides or {}
    key = f"{provider}/{model_name}"
    if key in overrides:
        return overrides[key]
    return _from_litellm(provider, model_name)


def lookup_context_limit(provider: str, model_name: str) -> int | None:
    price = _from_litellm(provider, model_name)
    if price is not None and price.max_input_tokens is not None:
        return price.max_input_tokens
    return None

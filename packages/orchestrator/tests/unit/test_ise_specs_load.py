from pathlib import Path

import yaml

from aidmi_orchestrator.strategy.base import make_strategy

SPECS = Path(__file__).parents[2] / "examples" / "strategy_specs"


def test_ise_specs_build():
    for fname in [
        "structured_selfcorrect_ise.yaml",
        "critique_ise.yaml",
        "freeform_ise.yaml",
    ]:
        spec = yaml.safe_load((SPECS / fname).read_text())
        strat = make_strategy(spec["strategy"], spec["config"])
        assert strat.config.validation_gate == "static"
        assert strat.config.fixer_model is not None

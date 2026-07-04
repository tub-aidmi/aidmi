import pytest
from pydantic import BaseModel
from aidmi_orchestrator.strategy.base import (
    Strategy, register_strategy, make_strategy, list_strategies,
)


class _DummyConfig(BaseModel):
    note: str = "x"


class _DummyStrategy:
    name = "dummy_xyz"

    def __init__(self, config: _DummyConfig):
        self.config = config

    async def generate(self, api):  # pragma: no cover
        raise NotImplementedError


def test_registry_round_trip():
    register_strategy("dummy_xyz", _DummyStrategy, _DummyConfig)
    s = make_strategy("dummy_xyz", {"note": "hi"})
    assert isinstance(s, _DummyStrategy)
    assert s.config.note == "hi"
    assert "dummy_xyz" in list_strategies()


def test_unknown_strategy_raises():
    with pytest.raises(ValueError, match="unknown strategy"):
        make_strategy("nope_not_real", {})


def test_double_register_raises():
    class A:
        def __init__(self, cfg): pass
    register_strategy("test_double_xyz", A, _DummyConfig)
    with pytest.raises(ValueError, match="already registered"):
        register_strategy("test_double_xyz", A, _DummyConfig)


from aidmi_orchestrator.fixtures.base import get_fixture, list_fixtures


def test_mock_fixture_registered():
    import aidmi_orchestrator.fixtures  # triggers registration
    assert "mock" in list_fixtures()
    f = get_fixture("mock")
    assert f.source_schema == "fixture_mock_src"
    assert f.source_sql_path.exists()
    assert f.reference_dbt_path is not None and f.reference_dbt_path.is_dir()


def test_master_fixture_registered():
    import aidmi_orchestrator.fixtures  # triggers registration
    assert "master" in list_fixtures()
    f = get_fixture("master")
    assert f.source_schema == "fixture_master_src"
    assert f.source_sql_path.exists()
    assert f.reference_dbt_path is None


def test_mock_strategy_registered_and_instantiable():
    import aidmi_orchestrator.strategy  # triggers registration
    from aidmi_orchestrator.strategy.base import make_strategy, list_strategies
    assert "mock" in list_strategies()
    s = make_strategy("mock", {"mapping_source": "/dev/null"})
    assert s.name == "mock"


def test_sql_fixtures_all_registered():
    import aidmi_orchestrator.fixtures  # noqa: F401
    for name in (
        "mock",
        "master",
        "wrong_field_names",
        "messy_data",
        "missing_relations",
        "wrong_field_names_v2",
        "messy_data_v2",
        "missing_relations_v2",
        "master_v2",
    ):
        fx = get_fixture(name)
        assert fx.source_schema == f"fixture_{name}_src"


def test_v2_fixtures_have_golden_schema_and_ground_truth():
    import aidmi_orchestrator.fixtures  # noqa: F401
    for name in (
        "wrong_field_names_v2",
        "messy_data_v2",
        "missing_relations_v2",
        "master_v2",
    ):
        fx = get_fixture(name)
        assert fx.golden_schema == f"fixture_{name}_golden"
        assert fx.source_sql_path.exists()
        assert fx.destination_sql_path.exists()
        assert fx.target_schema_path is not None and fx.target_schema_path.exists()
        assert "_ground_truth" in fx.destination_sql_path.read_text(encoding="utf-8")
        assert "ground_truth_recall" in fx.applicable_evaluators
        assert "ground_truth_notes" in fx.applicable_evaluators
        assert "ground_truth_field_accuracy" in fx.applicable_evaluators

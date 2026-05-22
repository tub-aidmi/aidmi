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


def test_sp1_users_fixture_registered():
    import aidmi_orchestrator.fixtures  # triggers registration
    assert "sp1_users" in list_fixtures()
    f = get_fixture("sp1_users")
    assert f.applicable_evaluators == ["execution", "llm_usage", "schema", "row_equality"]
    assert f.target_schema_path is not None and f.target_schema_path.exists()
    assert f.reference_dbt_path is not None and f.reference_dbt_path.is_dir()


def test_sf_pipedrive_fixture_registered():
    import aidmi_orchestrator.fixtures  # triggers registration

    assert "sf_pipedrive" in list_fixtures()
    f = get_fixture("sf_pipedrive")
    assert f.applicable_evaluators == ["execution", "llm_usage", "schema"]
    assert f.target_schema_path is not None and f.target_schema_path.exists()
    assert f.reference_dbt_path is None


def test_sf_pipedrive_load_source_requires_env(monkeypatch):
    import os

    import aidmi_orchestrator.fixtures  # noqa: F401 — triggers registration

    for k in list(os.environ):
        if k.startswith("SF_"):
            monkeypatch.delenv(k, raising=False)

    f = get_fixture("sf_pipedrive")
    with pytest.raises(RuntimeError, match="Missing Salesforce credentials"):
        f.source_factory()


def test_sf_fixture_source_factory_requires_security_token(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("SF_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SF_USERNAME", "u")
    monkeypatch.setenv("SF_PASSWORD", "p")

    import aidmi_orchestrator.fixtures  # noqa: F401

    f = get_fixture("sf_pipedrive")
    with pytest.raises(RuntimeError, match="Missing Salesforce credentials"):
        f.source_factory()


def test_sf_fixture_salesforce_explicit_credentials(monkeypatch):
    import os
    from unittest.mock import MagicMock, patch

    for k in list(os.environ):
        if k.startswith("SF_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SF_USERNAME", "u")
    monkeypatch.setenv("SF_PASSWORD", "p")
    monkeypatch.setenv("SF_SECURITY_TOKEN", "tok")

    import aidmi_orchestrator.fixtures  # noqa: F401
    import aidmi_orchestrator.fixtures.sf_pipedrive.salesforce.fixture_source as sf_mod

    with patch.object(sf_mod, "Salesforce", autospec=False) as mock_salesforce:
        mock_salesforce.return_value = MagicMock()
        get_fixture("sf_pipedrive").source_factory()
        assert mock_salesforce.call_args.kwargs["username"] == "u"
        assert mock_salesforce.call_args.kwargs["password"] == "p"
        assert mock_salesforce.call_args.kwargs["security_token"] == "tok"
        assert mock_salesforce.call_args.kwargs.keys() == {
            "username",
            "password",
            "security_token",
        }


def test_mock_strategy_registered_and_instantiable():
    import aidmi_orchestrator.strategy  # triggers registration
    from aidmi_orchestrator.strategy.base import make_strategy, list_strategies
    assert "mock" in list_strategies()
    s = make_strategy("mock", {"mapping_source": "/dev/null"})
    assert s.name == "mock"

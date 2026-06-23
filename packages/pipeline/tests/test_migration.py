from types import SimpleNamespace

from aidmi_pipeline.migration import _overall_status, dbt_model_table_name


def test_dbt_model_table_name_strips_schema_prefix() -> None:
    assert dbt_model_table_name("schema.Account") == "Account"
    assert dbt_model_table_name("Account") == "Account"


def test_overall_status_empty_models_is_error() -> None:
    assert _overall_status([]) == "error"


def test_overall_status_all_success() -> None:
    models = [SimpleNamespace(status="success")]
    assert _overall_status(models) == "success"

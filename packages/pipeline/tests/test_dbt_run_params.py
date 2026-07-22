from aidmi_pipeline.migration import _dbt_run_params


def test_fail_fast_true_adds_flag():
    assert _dbt_run_params(True) == ["--fail-fast"]


def test_fail_fast_false_is_empty():
    assert _dbt_run_params(False) == []

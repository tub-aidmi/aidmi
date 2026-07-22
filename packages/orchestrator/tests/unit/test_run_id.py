from aidmi_orchestrator.run_id import MAX_RUN_ID_LEN, make_run_id, slug
from aidmi_pipeline.config import out_schema_for_run


def test_make_run_id_format():
    run_id = make_run_id("write_tools_freeform", "master")
    assert run_id.startswith("r")
    hash8 = run_id[1:9]
    assert run_id[9] == "_"
    assert len(hash8) == 8
    assert hash8.isalnum()
    assert hash8 == hash8.lower()
    assert run_id.endswith("_master")
    assert run_id[10 : -len("_master")] == "write_tools_freeform"


def test_slug_sanitizes_special_chars():
    assert slug("Hello World!") == "hello_world"
    assert slug("  ") == "val"


def test_make_run_id_sanitizes_inputs():
    run_id = make_run_id("Plan Then Execute", "My Fixture")
    assert run_id.startswith("r")
    hash8 = run_id[1:9]
    assert len(hash8) == 8
    assert run_id.endswith("_my_fixture")
    assert run_id[10 : -len("_my_fixture")] == "plan_then_execute"


def test_truncation_when_names_exceed_postgres_limit():
    long_strategy = "a" * 80
    long_fixture = "b" * 40
    run_id = make_run_id(long_strategy, long_fixture)
    assert len(run_id) <= MAX_RUN_ID_LEN
    assert run_id.startswith("r")
    assert run_id.endswith(f"_{'b' * 40}")


def test_out_schema_equals_run_id_lower():
    run_id = make_run_id("structured_per_table", "master")
    assert out_schema_for_run(run_id) == run_id.lower()
    assert len(out_schema_for_run(run_id)) <= MAX_RUN_ID_LEN
    assert out_schema_for_run(run_id)[0].isalpha()

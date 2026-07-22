"""Pure-function tests for FK legacy-resolution helpers."""

from __future__ import annotations

from aidmi_orchestrator.evaluator._ground_truth_utils import (
    DANGLING,
    FK_COLUMN_NAMES,
    compare_matched_rows,
    index_by_id,
    resolve_fk,
)


def test_index_by_id_maps_surrogate_to_legacy():
    rows = [
        {"Id": "001A", "Legacy_Customer_ID__c": "CUST-1"},
        {"Id": "001B", "Legacy_Customer_ID__c": "CUST-2"},
        {"Id": None, "Legacy_Customer_ID__c": "CUST-3"},
        {"Id": "001D", "Legacy_Customer_ID__c": None},
    ]
    assert index_by_id(rows, "Legacy_Customer_ID__c") == {
        "001A": "CUST-1",
        "001B": "CUST-2",
    }


def test_resolve_fk_none_is_none():
    assert resolve_fk(None, {"001A": "CUST-1"}) is None


def test_resolve_fk_present_returns_legacy():
    assert resolve_fk("001A", {"001A": "CUST-1"}) == "CUST-1"


def test_resolve_fk_absent_is_dangling():
    assert resolve_fk("999X", {"001A": "CUST-1"}) is DANGLING


def test_compare_matched_rows_skips_fk_columns():
    assert "AccountId" in FK_COLUMN_NAMES
    golden = {"Legacy_Customer_ID__c": "C1", "Name": "Acme", "AccountId": "001A"}
    produced = {"Legacy_Customer_ID__c": "C1", "Name": "Acme", "AccountId": "999Z"}
    results = compare_matched_rows(golden, produced)
    assert "AccountId" not in results
    assert results["Name"] is True

from aidmi_orchestrator.evaluator._ground_truth_utils import row_is_field_correct


def test_strict_row_requires_all_compared_columns_match():
    golden = {"Legacy_Customer_ID__c": "A1", "Name": "Acme", "Region__c": "EU"}
    exact = {"Legacy_Customer_ID__c": "A1", "Name": "Acme", "Region__c": "EU"}
    wrong = {"Legacy_Customer_ID__c": "A1", "Name": "Acme", "Region__c": "US"}
    assert row_is_field_correct(golden, exact) is True
    assert row_is_field_correct(golden, wrong) is False


def test_strict_row_ignores_skip_columns():
    golden = {"Legacy_Customer_ID__c": "A1", "Name": "Acme", "Id": "x"}
    produced = {"Legacy_Customer_ID__c": "A1", "Name": "Acme", "Id": "DIFFERENT"}
    assert row_is_field_correct(golden, produced) is True

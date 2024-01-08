# test_data_quality.py

import pytest
import great_expectations as ge
from great_expectations.data_context import DataContext

def test_data_quality():
    data_context = DataContext()

    # Load a batch of data (replace with how you load your data)
    batch = ge.read_csv('your_test_data.csv')

    # Run validation
    results = data_context.run_validation_operator(
        "action_list_operator", 
        assets_to_validate=[batch],
        run_id="test-001"  # An identifier for this validation run
    )

    # Assert if the validation was successful
    assert results["success"], "Data quality validation failed"

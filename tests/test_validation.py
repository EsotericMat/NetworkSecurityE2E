from typing import assert_type

import numpy as np
import pytest
import os
import pandas as pd
import yaml

from network_security.pipes.validation import DataValidationPipe

REPORT_FILE = 'tests/artifacts/test_report.yaml'
@pytest.fixture
def get_validation_obj():
    return DataValidationPipe()

@pytest.fixture
def get_test_df():
    return pd.DataFrame({
        "col_a": [1, 0, -1],
        "col_b": [-1, 0, 1]
    })

@pytest.fixture
def get_null_test_df():
    return pd.DataFrame({
        "col_a": [1, 0, -1],
        "col_b": [-1, None, 1]
    })



def test_step_report(get_validation_obj):
    flag = get_validation_obj.step_report(
        subset_id='pytest_subset',
        step_name='module_testing',
        valid=True,
        report_dir= REPORT_FILE
    )
    assert flag
    assert os.path.exists(REPORT_FILE)
    with open(REPORT_FILE, 'r') as report:
        report_dict = yaml.safe_load(report)
        assert isinstance(report_dict, dict)
        assert report_dict['pytest_subset_module_testing']


def test_load_train_and_test(get_validation_obj):
    X_train, X_test = get_validation_obj.load_train_test_data()
    assert isinstance(X_train, pd.DataFrame)
    assert isinstance(X_test, pd.DataFrame)
    assert X_train.shape[0] >= X_test.shape[0]
    assert X_train.shape[1] == X_test.shape[1]
    assert 'Result' in X_train.columns
    assert 'Result' in X_train.columns


def test_validate_no_nulls(get_validation_obj, get_test_df, get_null_test_df):
    status = get_validation_obj.validate_no_nulls(get_test_df)
    assert status
    assert isinstance(status, str)
    assert status == 'True'

    nulls_status = get_validation_obj.validate_no_nulls(get_null_test_df)
    assert nulls_status
    assert isinstance(nulls_status, str)
    assert nulls_status == 'False'




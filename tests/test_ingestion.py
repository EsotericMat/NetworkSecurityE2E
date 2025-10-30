from unittest.mock import patch

import pandas as pd
import pytest
import os
from network_security.pipes.ingestion import DataIngestionPipe



@pytest.fixture
def get_ingestion_obj():
    return DataIngestionPipe()

# @patch('network_security.pipes.ingestion.DataIngestionPipe')
def test_read_from_mongo(get_ingestion_obj):
    df = get_ingestion_obj.read_from_mongo_db()
    assert isinstance(df, pd.DataFrame)
    assert df.shape
    assert df.shape[0] > 0
    assert df.shape[1] > 0
    assert '_id' not in df.columns
    assert 'Result' in df.columns


import pytest
import os
import certifi
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from network_security.pipes.etl import ETL

load_dotenv(verbose=True)

@pytest.fixture
def mongo_client():
    uri = os.environ['MONGO_URI']
    return MongoClient(uri, server_api=ServerApi('1'), tls=True, tlsCAFile=certifi.where())

@pytest.fixture
def get_ns_object():
    return ETL(database=os.environ['MONGO_DB_NAME'], collection=os.environ['MONGO_COLLECTION'])

@pytest.fixture
def get_set():
    return pd.read_csv('data/phishingData.csv', nrows=1)


def test_mongo_db_connection(mongo_client):
    # Create a new client and connect to the server
    client = mongo_client
    # Send a ping to confirm a successful connection
    try:
        assert client.admin.command('ping')
    except Exception as e:
        print(e)

def test_csv_to_json(get_ns_object):
    records = get_ns_object.csv_to_json('data/phishingData.csv', nrows=1)
    assert type(records) == list
    assert len(records) == 1




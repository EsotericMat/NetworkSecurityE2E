import os
import certifi
import json
import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from network_security import logger

load_dotenv()

ca = certifi.where() # Certificate authority for MongoDB
MONGO_URI = os.environ['MONGO_URI']

class ETL:

    def __init__(self, database, collection):
        self.client = MongoClient(MONGO_URI,
                                  server_api=ServerApi('1'),
                                  tls=True,
                                  tlsCAFile=certifi.where())
        self.database = self.client[database]
        self.collection = self.database[collection]

    def csv_to_json(self, path: str, nrows: int = None):
        try:
            if nrows is None:
                df = pd.read_csv(path)
            else:
                df = pd.read_csv(path, nrows=nrows)
            df.reset_index(drop=True, inplace=True)
            records = json.loads(df.T.to_json()).values()
            records_list = list(records)
            return records_list

        except Exception as e:
            logger.error(e)

    def insert_into_mongodb(self, records):
        try:
            self.collection.insert_many(records)
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    logger.info('Loading data')
    file_path = '../../data/phisingData.csv'
    etl_obj = ETL(database=os.environ['MONGO_DB_NAME'],
                  collection=os.environ['MONGO_COLLECTION'])
    logger.info('Transfer to JSON')
    records = etl_obj.csv_to_json(file_path)
    logger.info('Insert into MongoDB')
    etl_obj.insert_into_mongodb(records)
    logger.success('Successfully inserted into MongoDB')




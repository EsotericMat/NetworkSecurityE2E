"""
From MongoDB to Train and Test files
"""

import os
import pymongo
import certifi
import pandas as pd
from network_security import logger
from network_security.constant import ingestion
from network_security.pipes.etl import ETL
from network_security.toolkit.toolkit import make_dirs
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

ca = certifi.where() # Certificate authority for MongoDB
MONGO_URI = os.environ['MONGO_URI']

class DataIngestionPipe:

    def __init__(self):
        self.config = ingestion
        self.ingestion_dir: str = os.path.join(self.config.ARTIFACT_DIR,
                                               self.config.INGESTION_DIR)
        self.feature_store_dir: str = os.path.join(self.config.ARTIFACT_DIR,
                                                   self.config.FEATURE_STORE_DIR)
        self.train_dir: str = os.path.join(self.ingestion_dir,
                                           self.config.TRAIN_FILE_NAME)
        self.test_dir: str = os.path.join(self.ingestion_dir,
                                           self.config.TEST_FILE_NAME)

        make_dirs(self.ingestion_dir, self.feature_store_dir)

    def read_from_mongo_db(self) -> pd.DataFrame:
        """
        Read data from MongoDB collection, transfer to Pandas dataframe
        :return: pd.DataFrame
        """
        try:
            logger.info('Reading data from MongoDB')
            db_name = self.config.MONGO_DB_NAME
            collection_nama = self.config.MONGO_DB_COLLECTION
            etl_obj = ETL(db_name, collection_nama).collection
            df = pd.DataFrame(
                list(etl_obj.find({}))
            )
            if '_id' in list(df.columns):
                df.drop('_id', axis=1, inplace=True)
            return df

        except Exception as e:
            logger.exception(f"Can't read from MongoDB: {e}")

    def create_feature_store(self, df: pd.DataFrame) -> None:
        """
        Creat feature store object from pandas dataframe
        :param df: DataFrame to store
        :return: None
        """
        try:
            logger.info('Creating feature store')
            df.to_csv(f"{self.feature_store_dir}/{self.config.FEATURE_STORE_FILE}", index=False,header=True)
            logger.success(f'Feature store created at {self.feature_store_dir}')
        except Exception as e:
            logger.exception(f"Can't create feature store: {e}")
        return None

    def split_to_train_test(self, df: pd.DataFrame) -> None:
        """Split a given df to train and test"""
        try:
            logger.info('Split data into train and test')
            train_set, test_set = train_test_split(
                df,
                train_size=self.config.TRAIN_SIZE
            )
            train_set.to_csv(self.train_dir)
            test_set.to_csv(self.test_dir)
        except Exception as e:
            logger.exception(f"Can't split data into train and test: {e}")

    def run(self):
        try:
            logger.info('Starting data ingestion')
            df = self.read_from_mongo_db()
            self.create_feature_store(df=df)
            self.split_to_train_test(df)
            logger.success('Finished data ingestion')

        except Exception as e:
            logger.error(f"Can't finish data ingestion process: {e}")

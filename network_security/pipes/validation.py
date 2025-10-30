import os
import pandas as pd
import yaml
from typing import Any
from network_security import logger
from network_security.constant import validation
from network_security.toolkit.toolkit import make_dirs, read_yaml
from scipy.stats import ks_2samp

SUBSETS = ['train', 'test']

class DataValidationPipe:

    def __init__(self):
        self.config = validation
        self.schema_dir: str = os.path.join(self.config.ARTIFACT_DIR,
                                            self.config.SCHEMA_DIR)
        self.ingestion_dir: str = os.path.join(self.config.ARTIFACT_DIR,
                                               self.config.INGESTION_DIR)
        self.train_dir: str = os.path.join(self.ingestion_dir,
                                           self.config.TRAIN_FILE_NAME)
        self.test_dir: str = os.path.join(self.ingestion_dir,
                                          self.config.TEST_FILE_NAME)
        self.validation_dir: str = os.path.join(self.config.ARTIFACT_DIR,
                                                self.config.DATA_VALIDATION_VALID_DIR)
        self.validated_dir: str = os.path.join(self.validation_dir,
                                               self.config.DATA_VALIDATION_VALID_DIR)
        self.invalid_dir: str = os.path.join(self.validation_dir,
                                             self.config.DATA_VALIDATION_INVALID_DIR)
        self.validation_report_dir: str = os.path.join(self.validation_dir,
                                                       self.config.DATA_VALIDATION_DRIFT_REPORT_DIR)
        self.validation_report_file = self.validation_report_dir + "/" + self.config.DATA_VALIDATION_DRIFT_REPORT_FILE
        make_dirs(self.validation_dir,
                  self.validated_dir,
                  self.invalid_dir,
                  self.validation_report_dir)

    def step_report(self, subset_id: str, step_name: str, valid: bool = True, report_dir = None) -> Any:
        if not report_dir:
            report_dir = self.validation_report_file
        data = {
            f'{subset_id}_{step_name}': valid
        }
        report = read_yaml(report_dir)
        if report:
            report.update(data)
            with open(report_dir, 'w') as existing_report:
                yaml.dump(report, existing_report, default_flow_style=False)
            logger.info(f"Step report saved: {subset_id} - {step_name}: {valid}")

        else:
            report = data
            with open(report_dir, 'w') as existing_report:
                yaml.dump(data, existing_report, default_flow_style=False)
            logger.info(f"Step report saved: {subset_id} - {step_name}: {valid}")

        if valid:
            return 1
        return None

    def load_train_test_data(self):
        logger.info(f"Loading train and test data from {self.ingestion_dir}")
        train_x = pd.read_csv(self.train_dir)
        test_x = pd.read_csv(self.test_dir)
        return train_x, test_x

    def validate_no_nulls(self, X: pd.DataFrame):
        validation_status = X.isna().sum().sum() == 0
        return str(validation_status)

    def schema_validation(self, X: pd.DataFrame, remove_target=False):
        """
        Validate schema matching between ingested data and required schema
        :param X: dataset
        :param remove_target: if True, remove the target column (For test validation)
        :return:
        """
        try:
            X.drop('Unnamed: 0', axis=1, inplace=True)
            required_schema = read_yaml(self.schema_dir)
            given_schema = {k: v for k, v in zip(X.columns, X.dtypes)}
            if remove_target:
                del given_schema['Result']
            validation_status = required_schema == given_schema
            return str(validation_status)
        except Exception as e:
            logger.error(e)


    def validate_data_drift(self, X: pd.DataFrame,  X_base: pd.DataFrame, threshold: float = .05):
        try:
            report = {}
            for col in X_base.columns:
                d1 = X_base[col]
                d2 = X[col]
                dist_test = ks_2samp(d1, d2)
                if threshold <= dist_test.pvalue:
                    pass # These 2 columns are drawn from the same distribution
                else:
                    # These 2 columns are NOT drawn from the same distribution, mentioned them in the report:
                    report.update({col: round(dist_test.pvalue(), 4)})
                    pass
            if len(report) > 0:
                logger.warning(f'There is some drift in the following columns: {report.keys()}')
                return False, report
            return True, report

        except Exception as e:
            logger.warning(f'Data Drift test failed: {e}')


    def run(self):
        passed = 0
        logger.info("Starting data validation")
        X_train, X_test = self.load_train_test_data()

        for subset, nm in [(X_train,'train'), (X_test,'test')]:
            if self.step_report(nm,'Nulls_Detection', self.validate_no_nulls(subset)):
                passed += 1

            if self.step_report(nm,'Validation_Schema',self.schema_validation(subset)):
                passed += 1

        drift_status, report = self.validate_data_drift(X_train, X_test)
        if self.step_report('train and test', 'Drift', drift_status):
            passed += 1
        drift_dict = {'drift_report': report}
        validation_report = read_yaml(self.validation_report_file)
        validation_report.update(drift_dict)
        with open(self.validation_report_file, 'w') as existing_report:
            yaml.dump(validation_report, existing_report, default_flow_style=False)
        logger.info(f"Data drift report saved, Validation process done")
        if passed == 5:
            logger.success('Data validation passed')
            X_train.to_csv(f'{self.validated_dir}/{self.config.TRAIN_FILE_NAME}', index=False, header=True)
            X_test.to_csv(f'{self.validated_dir}/{self.config.TEST_FILE_NAME}', index=False, header=True)

        else:
            logger.error(f'Data validation failed, {3 - passed} tests failed. Check validation report')
            X_train.to_csv(f'{self.invalid_dir}/{self.config.TRAIN_FILE_NAME}', index=False, header=True)
            X_test.to_csv(f'{self.invalid_dir}/{self.config.TEST_FILE_NAME}', index=False, header=True)









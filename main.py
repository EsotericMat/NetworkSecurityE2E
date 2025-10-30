from network_security.pipes.ingestion import DataIngestionPipe
from network_security.pipes.validation import DataValidationPipe


if __name__ == '__main__':
    ingestionObj = DataIngestionPipe()
    validationObj = DataValidationPipe()

    ingestionObj.run()
    validationObj.run()
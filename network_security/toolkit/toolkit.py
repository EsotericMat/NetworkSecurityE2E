import os
import yaml
from network_security import logger
from pathlib import Path


def make_dirs(*dirs):
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def read_yaml(file_path: str | Path) :
    """
    load YAML files into a dictionary
    :param file_path: file directory
    :return:
    """
    try:
        with open(file_path, 'r') as yaml_file:
            data = yaml.safe_load(yaml_file)
            return data
    except FileNotFoundError as e:
        logger.warning(f"{e}: Can't read {file_path}")
        return None

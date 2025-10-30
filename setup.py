import os
from network_security import logger as log
from setuptools import find_packages, setup
from typing import List

def get_requirements() -> List[str]:
    """
    Return list of requirements
    :return:
    """
    log.info('Collecting Requirements')
    requirements = []
    try:
        with open("requirements.txt", 'r') as f:
            reqs = f.readlines()
            for req in reqs:
                req = req.strip()
                if req:
                    requirements.append(req)
        return requirements
    except FileNotFoundError:
        log.error('Requirements file not found')

setup(
    name='Network Security',
    version='0.0.1',
    author='Matan Stern',
    author_email='matanst7@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)
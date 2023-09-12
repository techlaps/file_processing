import os
from setuptools import setup, find_packages

version='2.0.0'
build_number=os.environ.get('BUILD_NUMBER')
wheel_version=f"{version}.{build_number}"
setup(
    name='file_processing',
    description='File Processing',
    packages=find_packages(exclude=['src.tests']),
    version=os.environ.get('BUILDNUMBER')
)
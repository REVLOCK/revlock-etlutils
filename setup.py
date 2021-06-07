from setuptools import setup

setup(
    name='etlutils',
    version='1.0.9',
    description='Utility class containing util functions for ETL',
    url='',
    author='Irfan Zufiqar',
    author_email='irfan@rev-lock.com',
    packages=['etlutils'],
    package_data={'etlutils': ['DefaultMappings.json']},
    zip_safe=False
)

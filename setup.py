from setuptools import setup

setup(
    name='etlutils',
    version='1.0.16',
    description='Utility class containing util functions for ETL',
    url='',
    author='Irfan Zulfiqar',
    author_email='irfan@rev-lock.com',
    packages=['etlutils'],
    package_data={'etlutils': ['DefaultMappings.json']},
    zip_safe=False
)

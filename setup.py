from setuptools import setup

setup(
    name='etlutils',
    version='1.0.6',
    description='Utility class containing util functions for ETL',
    url='',
    author='Juned Jabbar',
    author_email='jjabar@rev-lock.com',
    packages=['etlutils'],
    package_data={'etlutils': ['DefaultMappings.json']},
    zip_safe=False
)

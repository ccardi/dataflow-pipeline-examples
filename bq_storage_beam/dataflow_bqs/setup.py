import setuptools

REQUIRED_PACKAGES = [
    'google-cloud-bigquery-storage',
]

setuptools.setup(
    name='bqs_test',
    version='0.0.1',
    description='BQS set workflow package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)
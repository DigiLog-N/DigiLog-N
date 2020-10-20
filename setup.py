import setuptools
from glob import glob

with open("README.md", 'r') as f:
    long_description = f.read()

setuptools.setup(
        name='digilog_n',
        version='0.0.2',
        author='Canvass Labs Inc.',
        author_email='charlie@canvasslabs.com',
        description='DigiLog-N library',
        long_description=long_description,
        long_description_content_type='text/markdown',
        url='https://github.com/DigiLog-N/DataSource.git',
        packages=setuptools.find_packages(),
        scripts=glob('scripts/*'),
        # TODO: Embellish classifiers later
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OS Independent",
            ],
        python_requires='>=3.6',
        install_requires=['cassandra-driver', 'pandas', 'pyarrow', 'pymongo', 'pyspark', 'requests', 'pytz']
        )

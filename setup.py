"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='protseqspark',
    version='0.0.1',
    description='Functions and scripts to analyse protein sequences',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/benchiverton/ProteinSequences',
    packages=find_packages(),
    python_requires='>=3.5, <4',
    install_requires=[
        'pyspark',
        'biopython'
    ],
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    }
)

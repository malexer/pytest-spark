from setuptools import setup
from codecs import open
from os import path


cwd = path.abspath(path.dirname(__file__))


with open(path.join(cwd, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pytest-spark',
    version='0.1.1',

    description='pytest plugin to run the tests with support of pyspark.',
    long_description=long_description,
    url='https://github.com/malexer/pytest-spark',

    author='Alex (Oleksii) Markov',
    author_email='alex@markovs.me',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Pytest',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Testing',
    ],

    keywords=('pytest spark pyspark unittest test'),

    install_requires=['pytest', 'findspark'],
    entry_points={
        'pytest11': [
            'spark = pytest_spark',
        ],
    },
)

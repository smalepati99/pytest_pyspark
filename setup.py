from setuptools import setup
from codecs import open
from os import path

cwd = path.abspath(path.dirname(__file__))

with open(path.join(cwd, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(

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
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Testing',
    ],

    install_requires=['pytest', 'findspark'],
    packages=['pytest_spark'],
    entry_points={
        'pytest11': [
            'spark = pytest_spark',
        ],
    },
)

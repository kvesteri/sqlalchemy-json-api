"""
SQLAlchemy-JSON-API
-------------------
Fast SQLAlchemy query builder for returning JSON API responses
"""
from setuptools import setup, find_packages
import os
import re
import sys


HERE = os.path.dirname(os.path.abspath(__file__))
PY3 = sys.version_info[0] == 3


def get_version():
    filename = os.path.join(HERE, 'sqlalchemy_json_api', '__init__.py')
    with open(filename) as f:
        contents = f.read()
    pattern = r"^__version__ = '(.*?)'$"
    return re.search(pattern, contents, re.MULTILINE).group(1)


extras_require = {
    'test': [
        'pytest>=2.7.2',
        'Pygments>=1.2',
        'six>=1.4.1',
        'psycopg2>=2.6.1',
        'flake8>=2.4.0',
        'isort==3.9.6',
        'natsort==3.5.6',
    ],
}


setup(
    name='SQLAlchemy-JSON-API',
    version=get_version(),
    url='https://github.com/kvesteri/sqlalchemy-json-api',
    license='BSD',
    author='Konsta Vesterinen',
    author_email='konsta@fastmonkeys.com',
    description=(
        'Fast SQLAlchemy query builder for returning JSON API responses.'
    ),
    long_description=__doc__,
    packages=find_packages('.'),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    dependency_links=[],
    install_requires=[
        'SQLAlchemy-Utils>=0.30.17'
    ],
    extras_require=extras_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

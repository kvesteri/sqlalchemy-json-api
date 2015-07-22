Installation
------------

This part of the documentation covers the installation of SQLAlchemy-JSON-API.

Supported platforms
~~~~~~~~~~~~~~~~~~~

SQLAlchemy-JSON-API has been tested against the following Python platforms.

- cPython 2.6
- cPython 2.7
- cPython 3.3
- cPython 3.4


Installing an official release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can install the most recent official SQLAlchemy-JSON-API version using
pip_::

    pip install sqlalchemy-json-api

.. _pip: http://www.pip-installer.org/

Installing the development version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install the latest version of SQLAlchemy-JSON-API, you need first obtain a
copy of the source. You can do that by cloning the git_ repository::

    git clone git://github.com/kvesteri/sqlalchemy-json-api.git

Then you can install the source distribution using the ``setup.py``
script::

    cd sqlalchemy-json-api
    python setup.py install

.. _git: http://git-scm.org/

Checking the installation
~~~~~~~~~~~~~~~~~~~~~~~~~

To check that SQLAlchemy-JSON-API has been properly installed, type ``python``
from your shell. Then at the Python prompt, try to import SQLAlchemy-JSON-API,
and check the installed version:

.. parsed-literal::

    >>> import sqlalchemy_json_api
    >>> sqlalchemy_json_api.__version__
    |release|

.. The author disclaims copyright to this source code.

tbucket documentation
=====================

tbucket |version| last updated |today|

Installation
------------

This module depends on apsw, which is excellent but persnickety about build
configuration. The intended way to install apsw is via the system package
manager. The PyPI releases of apsw are not maintained by its author, but rather
by an unrelated third party. As of writing, these releases are undermaintained.

To avoid pulling broken builds from PyPI, apsw is not included in tbucket's
dependencies. You should install apsw from your system package manager, or
follow apsw's pip-based build instructions.

**TL;DR**:

  Do one of the following::

    sudo apt-get install python-apsw
    pip install tbucket

  or::

    pip install --user https://github.com/rogerbinns/apsw/releases/download/3.22.0-r1/apsw-3.22.0-r1.zip \
    --global-option=fetch --global-option=--version --global-option=3.22.0 --global-option=--all \
    --global-option=build --global-option=--enable-all-extensions
    pip install tbucket

For more information, see `apsw's documentation
<https://rogerbinns.github.io/apsw/download.html>`__.

Documentation
-------------

.. automodule:: tbucket
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:

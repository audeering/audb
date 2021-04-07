Contributing
============

If you would like to add new functionality fell free to create a `merge
request`_ . If you find errors, omissions, inconsistencies or other things
that need improvement, please create an issue_.
Contributions are always welcome!

.. _issue:
    https://gitlab.audeering.com/tools/audb/issues/new?issue%5BD=
.. _merge request:
    https://gitlab.audeering.com/tools/audb/merge_requests/new

Development Installation
------------------------

Instead of pip-installing the latest release from PyPI, you should get the
newest development version from Gitlab_::

    git clone git@srv-app-01.audeering.local:tools/audb.git
    cd audb
    # Use virutal environment
    pip install -r requirements.txt

.. _Gitlab: https://gitlab.audeering.com/tools/audb

This way, your installation always stays up-to-date, even if you pull new
changes from the Gitlab repository.

Building the Documentation
--------------------------

If you make changes to the documentation, you can re-create the HTML pages
using Sphinx_.
You can install it and a few other necessary packages with::

    pip install -r requirements.txt
    pip install -r docs/requirements.txt

To create the HTML pages, use::

	python -m sphinx docs/ build/sphinx/html -b html

The generated files will be available in the directory ``build/sphinx/html/``.

.. Note::

    During the default building of the documentation
    Jupyter notebooks are not executed to save time.

To execute the notebooks as well, copy and paste
the following into your terminal and press the enter key::

   python -m sphinx -W docs/ -D nbsphinx_execute='always' build/sphinx/html -b html

It is also possible to automatically check if all links are still valid::

    python -m sphinx docs/ build/sphinx/linkcheck -b linkcheck

.. _Sphinx: http://sphinx-doc.org/

Running the Tests
-----------------

You'll need pytest_ for that.
It can be installed with::

    pip install -r tests/requirements.txt

To execute the tests, simply run::

    pytest

To run the tests on the Gitlab CI server,
contributors have to make sure
they have an existing ``artifactory-tokenizer`` repository
with the content described in the `Artifactory tokenizer example`_.

.. _pytest:
    https://pytest.org/
.. _Artifactory tokenizer example:
    http://devops.pp.audeering.com/focustalks/2019-focustalk-artifactory-security/#tokenizer-example

Creating a New Release
----------------------

New releases are made using the following steps:

#. Update ``CHANGELOG.rst``
#. Commit those changes as "Release X.Y.Z"
#. Create an (annotated) tag with ``git tag -a vX.Y.Z``
#. Make sure you have an ``artifactory-tokenizer`` with ``deployers`` group
   permissions
#. Push the commit and the tag to Gitlab

.. _PyPI: https://artifactory.audeering.com/artifactory/api/pypi/pypi-local/simple/
.. _twine: https://twine.readthedocs.io/

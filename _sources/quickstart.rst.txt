.. Specify repository to overwrite local config files
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb

    audb.config.REPOSITORIES = [
        audb.Repository(
            name="data-public",
            host="https://audeering.jfrog.io/artifactory",
            backend="artifactory",
        )
    ]

.. _quickstart:

Quickstart
==========

The most common task is to load a database
with :func:`audb.load`.

Let's first see which databases are available
on our `public Artifactory server`_.


.. jupyter-execute::

    import audb

    audb.available(only_latest=True)

Let's load version 1.4.1 of the emodb_ database.

.. Load with only_metadata=True in the background
.. jupyter-execute::
    :hide-code:

    db = audb.load(
        "emodb",
        version="1.4.1",
        only_metadata=True,
        verbose=False,
    )

.. code-block:: python

    db = audb.load("emodb", version="1.4.1", verbose=False)

This downloads the database header,
all the media files,
and tables with annotations
to a caching folder on your machine.
The database is then returned
as an :class:`audformat.Database` object.

Each database comes with a description,
which is a good starting point
to learn what the database is all about.

.. jupyter-execute::

    db.description

The annotations of a database are stored in
tables represented by :class:`audformat.Table`.

.. jupyter-execute::

    db.tables

Each table contains columns (:class:`audformat.Column`)
that have corresponding schemes (:class:`audformat.Scheme`)
describing its content.
For example,
to get an idea about the emotion annotations
stored in the ``emotion`` column,
we can inspect the corresponding scheme.

.. jupyter-execute::

    db.schemes["emotion"]

Finally, we get the actual annotations
as a :class:`pandas.DataFrame`.

.. jupyter-execute::

    df = db["emotion"].get()  # get table
    df[:3]  # show first three entries


.. _emodb: https://github.com/audeering/emodb
.. _public Artifactory server: https://audeering.jfrog.io/artifactory

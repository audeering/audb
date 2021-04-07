.. Specify pandas format output in cells
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb
    import pandas as pd

    pd.set_option('display.max_columns', 7)


.. _database-dependencies:

Database dependencies
=====================

Media and table files of databases are stored
in archive files.
A database can also reuse an archive file
from a previous version of a database
if its content hasn't changed.

We keep track of those dependencies
and store some additional metadata about the audio files
like duration and number of channels
in a dependency table in a file :file:`db.csv`
for every version of a database.

You request a :class:`audb.Dependencies` object with:

.. jupyter-execute::

    deps = audb.dependencies('emodb', version='1.1.0')

You can see all entries with

.. jupyter-execute::

    deps()

or request certain aspects, e.g.

.. jupyter-execute::

    deps.duration('wav/03a01Fa.wav')

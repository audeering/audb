.. Specify repository to overwrite local config files
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb

    audb.config.REPOSITORIES = [
        audb.Repository(
            name='data-public',
            host='https://audeering.jfrog.io/artifactory',
            backend='artifactory',
        )
    ]

.. Specify pandas format output in cells
.. jupyter-execute::
    :hide-code:
    :hide-output:

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

You request a :class:`audb.Dependencies` object with
:func:`audb.dependencies`.

.. jupyter-execute::

    deps = audb.dependencies('emodb', version='1.3.0')

You can see all entries by calling the returned object.

.. jupyter-execute::

    df = deps()
    df.head()

You can also use it to request certain aspects, e.g.

.. jupyter-execute::

    deps.duration('wav/03a01Fa.wav')

See :class:`audb.Dependencies` for all available methods.


Duration of a database
----------------------

If your database contains only WAV or FLAC files,
we store the duration in seconds of every file
in the database dependency table.

.. jupyter-execute::

    deps = audb.dependencies('emodb', version='1.3.0')
    df = deps()
    df.duration[:10]

For those databases
you can get their overall duration with:

.. jupyter-execute::

    audb.info.duration('emodb', version='1.3.0')

The duration of parts of a database
can be calculated
by first loading the dependency table
and filter for the selected media files.
The following calculates the duration
of the first ten files in the *emotion* table
of the emodb database.

.. jupyter-execute::

    import numpy as np

    df = audb.load_table('emodb', 'emotion', version='1.3.0', verbose=False)
    files = df.index[:10]
    duration_in_sec = np.sum([deps.duration(f) for f in files])
    pd.to_timedelta(duration_in_sec, unit='s')

If your table is a segmented table,
and you would like to get the duration
of its segments
that contain a label
you can use :func:`audformat.utils.duration`,
which calculates the duration
from the ``start`` and ``end`` entries.

.. code-block:: python

    df = audb.load_table('database-with-segmented-tables', 'segmented-table')
    audformat.utils.duration(df.dropna())

Or you can count the duration of all segments within your database.

.. code-block:: python

    db = audb.load('database-with-segmented-tables', only_metadata=True)
    audformat.utils.duration(db.segments)

If your database contains files
for which no duration information is stored
in the dependency table of the database,
like MP4 files,
you have to download the database first
and use :func:`audformat.utils.duration`
to calculate the duration on the fly.

.. code-block:: python

    db = audb.load('database-with-videos')
    audformat.utils.duration(db.files, num_workers=4)

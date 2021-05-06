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


    def series_to_html(self):
        df = self.to_frame()
        df.columns = ['']
        return df._repr_html_()


    def index_to_html(self):
        return self.to_frame(index=False)._repr_html_()


    setattr(pd.Series, '_repr_html_', series_to_html)
    setattr(pd.Index, '_repr_html_', index_to_html)
    pd.set_option('display.max_rows', 6)


.. _load:

Load a database
===============

To load a database you only need its name.
However,
we recommend to specify its version as well.
This is not needed,
as :func:`audb.load` searches automatically
for the latest available version,
but it will ensure your code returns the same data,
even if a new version of the database is published.

.. Prefetch data with only_metadata=True
.. jupyter-execute::
    :hide-code:

    db = audb.load(
        'emodb',
        version='1.1.1',
        only_metadata=True,
        verbose=False,
    )

.. code-block:: python

    db = audb.load(
        'emodb',
        version='1.1.1',
        verbose=False,
    )

:func:`audb.load` will download the data,
store them in a cache folder,
and return the database as an :class:`audformat.Database` object.
The most important content of that object
are the database tables.

.. jupyter-execute::

    db.tables

They contain the annotations of the database,
and can be requested as a :class:`pandas.DataFrame`.

.. jupyter-execute::

    db['emotion'].get()

Or you can directly request single columns as :class:`pandas.Series`.

.. jupyter-execute::

    db['files']['duration'].get()

As you can see the index of the returned object
holds the path to the corresponding media files.

For a full overview how to handle the database object
we refer the reader to the `corresponding audformat documentation`_.
We also recommend to make you familiar
how to `combine tables`_
and how to `map labels`_.

Here,
we continue with discussing
:ref:`media-conversion-and-flavors`,
how to load :ref:`metadata`,
and :ref:`filter`.


.. _media-conversion-and-flavors:

Media conversion and flavors
----------------------------

When loading a database,
audio files can be automatically converted.
This creates a new flavor of the database,
represented by :class:`audb.Flavor`.
The following properties can be changed.

.. code-block:: yaml

    bit_depth:
      - 8
      - 16
      - 24
      - 32 (WAV only)
    format:
      - 'wav'
      - 'flac'
    channels:
      - 0        # select first channel
      - [0, -1]  # select first and last channel
      - ...
    mixdown:
      - False
      - True
    sampling_rate:
      - 8000
      - 16000
      - 22500
      - 44100
      - 48000

The next example will convert the original files
to FLAC with a sampling rate of 44100 Hz.
For each flavor a sub-folder will be created
inside the :ref:`cache <caching>`.

.. Prefetch data with only_metadata=True
.. jupyter-execute::
    :hide-code:

    db = audb.load(
        'emodb',
        version='1.1.1',
        format='flac',
        sampling_rate=44100,
        only_metadata=True,
        verbose=False,
    )

.. code-block:: python

    db = audb.load(
        'emodb',
        version='1.1.1',
        format='flac',
        sampling_rate=44100,
        verbose=False,
    )

The flavor information of a database is stored
inside the ``db.meta['audb']`` dictionary.

.. jupyter-execute::

    db.meta['audb']['flavor']

You can list all available flavors and their locations in the cache with:

.. jupyter-execute::

    df = audb.cached()
    df[['name', 'version', 'complete', 'format', 'sampling_rate']]

The entry ``'complete'`` tells you if a database flavor is completely cached,
or if some table or media files are still missing.


.. _metadata:

Metadata and header only
------------------------

It is possible to request only metadata
(header and annotations)
of a database.
In that case media files are not loaded,
but all the tables and the header.

.. jupyter-execute::

    db = audb.load(
        'emodb',
        version='1.1.1',
        only_metadata=True,
        verbose=False,
    )

For databases with many annotations,
this can still take some time.
If you are only interested in header information,
you can use :func:`audb.info.header`.
Or if you are only interested
in parts of the header,
have a look at the :mod:`audb.info` module.
It can list all table definitions.

.. jupyter-execute::

    audb.info.tables(
        'emodb',
        version='1.1.1',
    )

Or get the total duration of all media files.

.. jupyter-execute::

    audb.info.duration(
        'emodb',
        version='1.1.1',
    )

See :mod:`audb.info` for a list of all available options.


.. _filter:

Loading on demand
-----------------

It is possible to request only
specific tables or media of a database.

For instance, many databases are organized
into *train*, *dev*, and *test* splits.
Hence,
to evaluate the performance of a machine learning model,
we don't have to download the full database,
but only the table(s) and media of the *test* set.

Or, if we want the data of a specific speaker,
we can do the following.
First, we download the table with information
about the speakers (here ``db['files']``):

.. jupyter-execute::

    db = audb.load(
        'emodb',
        version='1.1.1',
        tables=['files'],
        only_metadata=True,
        full_path=False,
        verbose=False,
    )
    db.tables

Note,
that we set ``only_metadata=True``
since we only need the labels at the moment.
By setting ``full_path=False``
we further ensure that the paths
in the table index are relative
and therefore match the paths on the backend.

.. jupyter-execute::

    speaker = db['files']['speaker'].get()
    speaker

Now, we use the column with speaker IDs
to get a list of media files
that belong to speaker 3.

.. jupyter-execute::

    media = db['files'].files[speaker == 3]
    media

Finally, we load the database again
and use the list to request
only the data of this speaker.

.. Prefetch data with only_metadata=True
.. jupyter-execute::
    :hide-code:

    db = audb.load(
        'emodb',
        version='1.1.1',
        media=media,
        full_path=False,
        only_metadata=True,
        verbose=False,
    )

.. code-block:: python

    db = audb.load(
        'emodb',
        version='1.1.1',
        media=media,
        full_path=False,
        verbose=False,
    )

This will also remove
entries of other speakers
from the tables.

.. jupyter-execute::

    db['emotion'].get()


.. _corresponding audformat documentation: https://audeering.github.io/audformat/accessing-data.html
.. _combine tables: https://audeering.github.io/audformat/combine-tables.html
.. _map labels: https://audeering.github.io/audformat/map-scheme.html

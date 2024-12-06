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
.. invisible-code-block: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

.. skip: next

>>> db = audb.load("emodb", version="1.4.1", full_path=False, verbose=False)


:func:`audb.load` will download the data,
store them in a cache folder,
and return the database as an :class:`audformat.Database` object.
The most important content of that object
are the database tables.

>>> db.tables
emotion:
  type: filewise
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.test.gold_standard:
  type: filewise
  split_id: test
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.train.gold_standard:
  type: filewise
  split_id: train
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
files:
  type: filewise
  columns:
    duration: {scheme_id: duration}
    speaker: {scheme_id: speaker}
    transcription: {scheme_id: transcription}

They contain the annotations of the database,
and can be requested as a :class:`pandas.DataFrame`.

>>> db["emotion"].get()
                   emotion  emotion.confidence
file
wav/03a01Fa.wav  happiness                0.90
wav/03a01Nc.wav    neutral                1.00
wav/03a01Wa.wav      anger                0.95
wav/03a02Fc.wav  happiness                0.85
wav/03a02Nc.wav    neutral                1.00
...                    ...                 ...
wav/16b10Lb.wav    boredom                1.00
wav/16b10Tb.wav    sadness                0.90
wav/16b10Td.wav    sadness                0.95
wav/16b10Wa.wav      anger                1.00
wav/16b10Wb.wav      anger                1.00
<BLANKLINE>
[535 rows x 2 columns]

Or you can directly request single columns as :class:`pandas.Series`.

>>> db["files"]["duration"].get()
file
wav/03a01Fa.wav      0 days 00:00:01.898250
wav/03a01Nc.wav      0 days 00:00:01.611250
wav/03a01Wa.wav   0 days 00:00:01.877812500
wav/03a02Fc.wav      0 days 00:00:02.006250
wav/03a02Nc.wav   0 days 00:00:01.439812500
                            ...
wav/16b10Lb.wav   0 days 00:00:03.442687500
wav/16b10Tb.wav      0 days 00:00:03.500625
wav/16b10Td.wav   0 days 00:00:03.934187500
wav/16b10Wa.wav      0 days 00:00:02.414125
wav/16b10Wb.wav   0 days 00:00:02.522499999
Name: duration, Length: 535, dtype: timedelta64[ns]

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
      - 22050
      - 24000
      - 44100
      - 48000

The next example will convert the original files
to FLAC with a sampling rate of 44100 Hz.
For each flavor a sub-folder will be created
inside the :ref:`cache <caching>`.

.. Prefetch data with only_metadata=True
.. invisible-code-block: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        format="flac",
        sampling_rate=44100,
        only_metadata=True,
        verbose=False,
    )

.. skip: start

.. code-block:: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        format="flac",
        sampling_rate=44100,
        verbose=False,
    )

.. skip: end

The flavor information of a database is stored
inside the ``db.meta["audb"]`` dictionary.

>>> db.meta["audb"]["flavor"]
{'bit_depth': None,
 'channels': None,
 'format': 'flac',
 'mixdown': False,
 'sampling_rate': 44100}

You can list all available flavors and their locations in the cache with:

>>> df = audb.cached()
>>> df.reset_index()[["name", "version", "complete", "format", "sampling_rate"]]
         name version  complete format sampling_rate
0       emodb   1.4.1     False   flac         44100
1       emodb   1.4.1     False   None          None

The entry ``"complete"`` tells you if a database flavor is completely cached,
or if some table or media files are still missing.


.. _metadata:

Metadata and header only
------------------------

It is possible to request only metadata
(header and annotations)
of a database.
In that case media files are not loaded,
but all the tables and the header.

>>> db = audb.load("emodb", version="1.4.1", only_metadata=True, verbose=False)

For databases with many annotations,
this can still take some time.
If you are only interested in header information,
you can use :func:`audb.info.header`.
Or if you are only interested
in parts of the header,
have a look at the :mod:`audb.info` module.
It can list all table definitions.

>>> audb.info.tables("emodb", version="1.4.1")
emotion:
  type: filewise
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.test.gold_standard:
  type: filewise
  split_id: test
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.train.gold_standard:
  type: filewise
  split_id: train
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
files:
  type: filewise
  columns:
    duration: {scheme_id: duration}
    speaker: {scheme_id: speaker}
    transcription: {scheme_id: transcription}

Or get the total duration of all media files.

>>> audb.info.duration("emodb", version="1.4.1")
Timedelta('0 days 00:24:47.092187500')

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
about the speakers (here ``db["files"]``):

.. code-block:: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        tables=["files"],
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

>>> db.tables
files:
  type: filewise
  columns:
    duration: {scheme_id: duration}
    speaker: {scheme_id: speaker}
    transcription: {scheme_id: transcription}

Note,
that we set ``only_metadata=True``
since we only need the labels at the moment.
By setting ``full_path=False``
we further ensure that the paths
in the table index are relative
and therefore match the paths on the backend.

>>> speaker = db["files"]["speaker"].get()
>>> speaker
file
wav/03a01Fa.wav     3
wav/03a01Nc.wav     3
wav/03a01Wa.wav     3
wav/03a02Fc.wav     3
wav/03a02Nc.wav     3
                   ..
wav/16b10Lb.wav    16
wav/16b10Tb.wav    16
wav/16b10Td.wav    16
wav/16b10Wa.wav    16
wav/16b10Wb.wav    16
Name: speaker, Length: 535, dtype: category
Categories (10, int64): [3, 8, 9, 10, ..., 13, 14, 15, 16]

Now, we use the column with speaker IDs
to get a list of media files
that belong to speaker 3.


>>> media = db["files"].files[speaker == 3]
>>> media
Index(['wav/03a01Fa.wav', 'wav/03a01Nc.wav', 'wav/03a01Wa.wav',
       'wav/03a02Fc.wav', 'wav/03a02Nc.wav', 'wav/03a02Ta.wav',
       'wav/03a02Wb.wav', 'wav/03a02Wc.wav', 'wav/03a04Ad.wav',
       'wav/03a04Fd.wav', 'wav/03a04Lc.wav', 'wav/03a04Nc.wav',
       'wav/03a04Ta.wav', 'wav/03a04Wc.wav', 'wav/03a05Aa.wav',
       'wav/03a05Fc.wav', 'wav/03a05Nd.wav', 'wav/03a05Tc.wav',
       'wav/03a05Wa.wav', 'wav/03a05Wb.wav', 'wav/03a07Fa.wav',
       'wav/03a07Fb.wav', 'wav/03a07La.wav', 'wav/03a07Nc.wav',
       'wav/03a07Wc.wav', 'wav/03b01Fa.wav', 'wav/03b01Lb.wav',
       'wav/03b01Nb.wav', 'wav/03b01Td.wav', 'wav/03b01Wa.wav',
       'wav/03b01Wc.wav', 'wav/03b02Aa.wav', 'wav/03b02La.wav',
       'wav/03b02Na.wav', 'wav/03b02Tb.wav', 'wav/03b02Wb.wav',
       'wav/03b03Nb.wav', 'wav/03b03Tc.wav', 'wav/03b03Wc.wav',
       'wav/03b09La.wav', 'wav/03b09Nc.wav', 'wav/03b09Tc.wav',
       'wav/03b09Wa.wav', 'wav/03b10Ab.wav', 'wav/03b10Ec.wav',
       'wav/03b10Na.wav', 'wav/03b10Nc.wav', 'wav/03b10Wb.wav',
       'wav/03b10Wc.wav'],
      dtype='string', name='file')

Finally, we load the database again
and use the list to request
only the data of this speaker.

.. Prefetch data with only_metadata=True
.. invisible-code-block: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        media=media,
        full_path=False,
        only_metadata=True,
        verbose=False,
    )

.. skip: start

.. code-block:: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        media=media,
        full_path=False,
        verbose=False,
    )

.. skip: end

This will also remove
entries of other speakers
from the tables.

>>> db["emotion"].get().head()
                   emotion  emotion.confidence
file
wav/03a01Fa.wav  happiness                0.90
wav/03a01Nc.wav    neutral                1.00
wav/03a01Wa.wav      anger                0.95
wav/03a02Fc.wav  happiness                0.85
wav/03a02Nc.wav    neutral                1.00


.. _streaming:

Streaming
---------

:func:`audb.stream` provides a pseudo-streaming mode,
which helps to load large datasets.
It will only load ``batch_size`` number of rows
from a selected table into memory,
and download only matching media files
in each iteration.
The table and media files
are still stored in the cache.

.. Prefetch data with only_metadata=True
.. invisible-code-block: python

    db = audb.stream(
        "emodb",
        "emotion",
        version="1.4.1",
        batch_size=4,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

.. skip: start

.. code-block:: python

     db = audb.stream(
        "emodb",
        "emotion",
        version="1.4.1",
        batch_size=4,
        full_path=False,
        verbose=False,
    )

.. skip: end

It returns an :class:`audb.DatabaseIterator` object,
which behaves as :class:`audformat.Database`,
but provides the ability
to iterate over the database:

>>> next(db)
                   emotion  emotion.confidence
file
wav/03a01Fa.wav  happiness                0.90
wav/03a01Nc.wav    neutral                1.00
wav/03a01Wa.wav      anger                0.95
wav/03a02Fc.wav  happiness                0.85

With ``shuffle=True``,
a user can request
that the data is returned in a random order.
:func:`audb.stream` will then load ``buffer_size`` of rows
into an buffer and selected randomly from those.

.. code-block:: python

    import numpy as np
    np.random.seed(1)
    db = audb.stream(
        "emodb",
        "emotion",
        version="1.4.1",
        batch_size=4,
        shuffle=True,
        buffer_size=100_000,
        only_metadata=True,
        full_path=False,
        verbose=False,
    )

>>> next(db)
                   emotion  emotion.confidence
file
wav/14a05Fb.wav  happiness                 1.0
wav/15a05Eb.wav    disgust                 1.0
wav/12a05Nd.wav    neutral                 0.9
wav/13a07Na.wav    neutral                 0.9


.. _corresponding audformat documentation: https://audeering.github.io/audformat/accessing-data.html
.. _combine tables: https://audeering.github.io/audformat/combine-tables.html
.. _map labels: https://audeering.github.io/audformat/map-scheme.html

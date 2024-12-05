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
in a dependency table in a file :file:`db.parquet`
for every version of a database.

You request a :class:`audb.Dependencies` object with
:func:`audb.dependencies`.

>>> deps = audb.dependencies("emodb", version="1.4.1")

You can see all entries by calling the returned object.

>>> df = deps()
>>> df.head()
                                              archive  bit_depth  ...  type version
db.emotion.csv                                emotion          0  ...     0   1.1.0
db.files.csv                                    files          0  ...     0   1.1.0
wav/03a01Fa.wav  c1f5cc6f-6d00-348a-ba3b-4adaa2436aad         16  ...     1   1.1.0
wav/03a01Nc.wav  d40b53dd-8f0d-d5d3-42e7-8ad7ea6a05a6         16  ...     1   1.1.0
wav/03a01Wa.wav  9b62bc9b-a68e-7e38-6ed1-f4a16ac18511         16  ...     1   1.1.0
<BLANKLINE>
[5 rows x 10 columns]

You can also use it to request certain aspects, e.g.

>>> deps.duration("wav/03a01Fa.wav")
1.89825

See :class:`audb.Dependencies` for all available methods.


Duration of a database
----------------------

If your database contains only WAV or FLAC files,
we store the duration in seconds of every file
in the database dependency table.

..
    >>> import pandas as pd

.. skip: start if(pd.__version__ == "2.1.4", reason="formats output differently")

>>> deps = audb.dependencies("emodb", version="1.4.1")
>>> df = deps()
>>> df.duration[:10]
db.emotion.csv          0.0
db.files.csv            0.0
wav/03a01Fa.wav     1.89825
wav/03a01Nc.wav     1.61125
wav/03a01Wa.wav    1.877813
wav/03a02Fc.wav     2.00625
wav/03a02Nc.wav    1.439812
wav/03a02Ta.wav    1.735688
wav/03a02Wb.wav    2.123625
wav/03a02Wc.wav    1.498063
Name: duration, dtype: double[pyarrow]

.. skip: end

For those databases
you can get their overall duration with:

>>> audb.info.duration("emodb", version="1.4.1")
Timedelta('0 days 00:24:47.092187500')

The duration of parts of a database
can be calculated
by first loading the dependency table
and filter for the selected media files.
The following calculates the duration
of the first ten files in the *emotion* table
of the emodb database.

>>> import numpy as np
>>> import pandas as pd
>>> df = audb.load_table("emodb", "emotion", version="1.4.1", verbose=False)
>>> files = df.index[:10]
>>> duration_in_sec = np.sum([deps.duration(f) for f in files])
>>> pd.to_timedelta(duration_in_sec, unit="s")
Timedelta('0 days 00:00:17.392437500')

If your table is a segmented table,
and you would like to get the duration
of its segments
that contain a label
you can use :func:`audformat.utils.duration`,
which calculates the duration
from the ``start`` and ``end`` entries.

>>> import audformat
>>> df = audb.load_table("vadtoolkit", "segments", version="1.1.0", verbose=False)
>>> audformat.utils.duration(df.dropna())
Timedelta('0 days 00:37:17.037467')

Or you can count the duration of all segments within your database.

>>> db = audb.load("vadtoolkit", version="1.1.0", only_metadata=True, verbose=False)
>>> audformat.utils.duration(db.segments)
Timedelta('0 days 00:37:17.037467')

If your database contains files
for which no duration information is stored
in the dependency table of the database,
like MP4 files,
you have to download the database first
and use :func:`audformat.utils.duration`
to calculate the duration on the fly.

.. skip: start

>>> db = audb.load("database-with-videos")
>>> audformat.utils.duration(db.files, num_workers=4)

.. skip: end

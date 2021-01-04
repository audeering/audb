Usage
=====

.. Preload some data to avoid stderr print outs from tqdm,
.. but still avoid using the verbose=False flag later on

.. jupyter-execute::
    :stderr:
    :hide-output:
    :hide-code:

    import os
    import audb2
    import audeer

    audb2.load('emodb', version='1.0.1')
    audb2.load(
        'emodb',
        version='1.0.1',
        format='flac',
        sampling_rate=44100,
    )
    audb2.load('emodb', only_metadata=True)


Quickstart
----------

To use emodb_ (see `available databases`_ for more) in your project:

.. jupyter-execute::

    import audb2


    db = audb2.load('emodb', version='1.0.1')  # load database
    df = db['emotion'].get()  # get table
    df[:3]  # show first three entries

If you don't specify a version,
:mod:`audb2` will retrieve the latest version for you.
You can find the latest version with:

.. jupyter-execute::

    audb2.latest_version('emodb')

Or to get a list of all available versions, you can do:

.. jupyter-execute::

    audb2.versions('emodb')


Overview
--------

In the process of providing you the data,
:mod:`audb2`

1. finds the latest version of a database (optional)
2. calculates :ref:`database dependencies <database-dependencies>`
3. downloads archive files from Artifactory
4. unpacks the archive files
5. inspects and :ref:`converts <database-conversion-and-flavors>`
   the audio files (optional)
6. stores the data in a :ref:`cache <cache-root>` folder

.. graphviz:: pics/workflow.dot


.. _database-dependencies:

Database dependencies
---------------------

Databases stored in the `Unified Format`_
consists of one metadata archive file
and one up to several thousand archive files for data.
All of them are stored as dependencies on Artifactory,
that are resolved on-the-fly by :mod:`audb2`.

You can visualize dependencies of a database with:

.. jupyter-execute::

    depend = audb2.dependencies('emodb', version='1.0.1')
    depend()


.. _database-conversion-and-flavors:

Data conversion and flavors
---------------------------

When loading a database,
audio files can be automatically converted.
This creates a new **flavor** of the database.
The following properties can be changed:

.. code-block:: yaml

    bit_depth:
      - 8
      - 16
      - 24
      - 32 (WAV only)
    channels, e.g.:
      - 0        # select first channel
      - [0, -1]  # select first and last channel
    format:
      - 'wav'
      - 'flac'
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
inside the :ref:`cache <cache-root>`.

.. jupyter-execute::

    db = audb2.load(
        'emodb',
        version='1.0.1',
        format='flac',
        sampling_rate=44100,
    )

The new audio format is included in the flavor of the converted database:

.. jupyter-execute::

    db.meta['audb']['flavor']

You can list all available flavors with:

.. jupyter-execute::

    df = audb2.cached()
    df[['name', 'version', 'only_metadata', 'sampling_rate']]


Metadata and header only
------------------------

It is possible to request only metadata
(header and annotations)
of a database.
In that case audio files are not loaded,
but all the annotations and the header:

.. jupyter-execute::

    db = audb2.load(
        'emodb',
        version='1.0.1',
        only_metadata=True,
    )

For databases with many annotations,
this can still take some time.
If you are only interested in header information,
you can use :func:`audb2.info.header`,
or if you are only interested in table information:

.. jupyter-execute::

    audb2.info.tables(
        'emodb',
        version='1.0.1',
    )

See :mod:`audb2.info` for a list of all available options.


.. _cache-root:

Caching
-------

When you request a database the first time,
:mod:`audb2` will unpack (and convert) annotations and data to
:file:`<cache_root>/<repository>/<group_id>/<name>/<flavor>/<version>/`.
Next time your request it again,
:mod:`audb2` will directly load the database from there.

:mod:`audb2` distinguishes two :file:`<cache_root>` folders.
A shared cache folder, which we use on our `compute servers`_.
By default it is located under :file:`/data/audb2`
and can be accessed by all users.

.. jupyter-execute::

    audb2.default_cache_root(shared=True)

Please use the shared folder only
for all databases with public access level.
If you are unsure,
have a look at the list of `available databases`_.

The second cache folder should be
accessible to you only.
It is reserved for databases that
are not relevant to other users or
databases that have private access level.
By default it points to ``~/audb2``.

.. jupyter-execute::

    audb2.default_cache_root(shared=False)

When you request a database with :meth:`audb2.load`,
:mod:`audb2` first looks for it in the shared cache folder
and afterwards in your local cache folder.

There are two ways to change the default locations:

1. By setting the argument ``cache_root`` during a function call, e.g:

.. code-block:: python

    audb2.load('emodb', ..., cache_root='/cache/root/audb2')

2. Program-wide by overwriting the default values in :class:`audb2.config`:

.. code-block:: python

    audb2.config.CACHE_ROOT = '/new/local/cache/audb2'
    audb2.config.SHARED_CACHE_ROOT = '/new/shared/cache/audb2'

3. System-wide by setting the following system variables:

.. code-block:: bash

    export AUDB2_CACHE_ROOT=/new/local/cache/audb2
    export AUDB2_SHARED_CACHE_ROOT=/new/shared/cache/audb2

Note that 1. overwrites 2. and 3., and 3. overwrites 2.


.. _emodb:
    https://gitlab.audeering.com/data/emodb
.. _available databases:
    http://data.pp.audeering.com/databases.html
.. _Unified Format:
    http://tools.pp.audeering.com/audata/data-format.html
.. _POM files:
    https://maven.apache.org/guides/introduction/introduction-to-the-pom.html
.. _compute servers:
    https://gitlab.audeering.com/devops/computex

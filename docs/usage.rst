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
        mix='stereo',
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

If you don't specify a version,
:mod:`audb2` will retrieve the latest version for you.
You can visualize dependencies of a database with:

.. jupyter-execute::

    depend = audb2.Depend()
    depend_file = os.path.join(db.meta['audb']['root'], 'db.csv')
    depend.from_file(depend_file)
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
    format:
      - 'wav'
      - 'flac'
    mix:
      - 'mono'
      - 'mono-only'
      - 'left'
      - 'right'
      - 'stereo'
      - 'stereo-only'
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
        mix='stereo',
        sampling_rate=44100,
    )

The new audio format is included in the flavor of the converted database:

.. jupyter-execute::

    db.meta['audb']['flavor']

You can list all available flavors with:

.. jupyter-execute::

    df = audb2.cached_databases()
    df[['name', 'version', 'mix', 'sampling_rate']]



Metadata and header only
------------------------

It is possible to request only metadata
(header and annotations)
of a database.
In that case audio files are not loaded,
but all the annotations and the header:

.. jupyter-execute::

    db = audb2.load('emodb', only_metadata=True)


.. _cache-root:

Cache root
----------

:file:`<cache_root>` points to the local folder
where the databases are stored.
It's default value is ``~/audb2``.
When you request a database the first time,
:mod:`audb2` will:

* unpack (and convert) annotations and data to
  :file:`<cache_root>/<name>/<flavor>/<version>/`.

There are two ways to change :file:`cache_root`:

1. Explicitly, by setting a different ``cache_root``

.. code-block:: python

    audb2.config.CACHE_ROOT = '/new/cache/audb2'

2. Implicitly, through the system variable ``AUDB2_CACHE_ROOT``, e.g.:

.. code-block:: bash

    export AUDB2_CACHE_ROOT=/new/cache/audb2

Note that 1. overwrites 2.


Cache root on computeX
----------------------

On our `compute servers`_, we have a shared :mod:`audb2` cache folder
under :file:`/data/audb2`.
Please use this for all databases with public access level.
If you are unsure,
have a look at the list of `available databases`_.

Databases with private access levels
should never be stored under the shared cache folder.


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

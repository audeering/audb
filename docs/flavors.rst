.. Preload some data to avoid stderr print outs from tqdm,
.. but still avoid using the verbose=False flag later on

.. jupyter-execute::
    :stderr:
    :hide-output:
    :hide-code:

    import audb2


    audb2.load('emodb', version='1.0.1')
    audb2.load(
        'emodb',
        version='1.0.1',
        format='flac',
        sampling_rate=44100,
    )


.. _database-conversion-and-flavors:

Data conversion and flavors
===========================

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

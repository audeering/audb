.. Preload some data to avoid stderr print outs from tqdm,
.. but still avoid using the verbose=False flag later on

.. jupyter-execute::
    :stderr:
    :hide-output:
    :hide-code:

    import audb2


    audb2.load('emodb', only_metadata=True)


Metadata and header only
========================

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

And to get the total duration of all media files:

.. jupyter-execute::

    audb2.info.duration(
        'emodb',
        version='1.0.1',
    )

See :mod:`audb2.info` for a list of all available options.

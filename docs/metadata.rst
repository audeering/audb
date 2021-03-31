Metadata and header only
========================

It is possible to request only metadata
(header and annotations)
of a database.
In that case audio files are not loaded,
but all the annotations and the header:

.. jupyter-execute::

    import audb2


    db = audb2.load(
        'emodb',
        version='1.1.0',
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
        version='1.1.0',
    )

And to get the total duration of all media files:

.. jupyter-execute::

    audb2.info.duration(
        'emodb',
        version='1.1.0',
    )

See :mod:`audb2.info` for a list of all available options.

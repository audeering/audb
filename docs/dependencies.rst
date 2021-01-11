.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb2


.. _database-dependencies:

Database dependencies
=====================

Databases stored in the audformat_
consists of one metadata archive file
and one up to several thousand archive files for data.
All of them are stored as dependencies on Artifactory,
that are resolved on-the-fly by :mod:`audb2`.

You can visualize dependencies of a database with:

.. jupyter-execute::

    depend = audb2.dependencies('emodb', version='1.0.1')
    depend()

From the dependencies you can easily calculate the total
duration of all media files in the database:

.. jupyter-execute::

    import pandas as pd


    pd.to_timedelta(
        sum([depend.duration(file) for file in depend.media]),
        unit='s',
    )
    
.. _audformat: http://tools.pp.audeering.com/audata/data-format.html

Quickstart
==========

To use emodb_ (see `available databases`_ for more) in your project:

.. jupyter-execute::

    import audb2


    db = audb2.load('emodb', version='1.1.0', verbose=False)  # load database
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



.. _emodb: https://gitlab.audeering.com/data/emodb
.. _available databases: http://data.pp.audeering.com/databases.html

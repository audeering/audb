Overview
========

:mod:`audb` is similar to a version control system
for text and binary data.
It allows to manage your databases
for machine learning applications
and other tasks
where reproducibility
and easy combination of different data sources is needed.

The databases itself can be stored on different backends.
You can publish or download different versions
of the same database,
without the need to copy data
that hasn't changed between the different versions.

In the following we provide a technical overview
of the underlying workings of :mod:`audb`.
If you just want to use it,
you might read on at :ref:`publish`
or :ref:`load`.


Backends
--------

:mod:`audb` abstracts the database storage
by using the :mod:`audbackend` package
to communicate with the underlying backend.
At the moment,
it supports to store the data
in a folder on a local file system,
or inside a `Generic repository`_
on an `Artifactory server`_.

You could easily expand this,
by adding your own backend
that `implements the required functions`_.

Storage on backends are managed by :class:`audb.Repository`
objects.
For example,
to store all your data
on your local disk under :file:`/data/data-local`
you would use the following repository.

.. jupyter-execute::
    :hide-code:

    import audb

.. jupyter-execute::

    repository = audb.Repository(
        name='data-local',
        host='/data',
        backend='file-system',
    )

The default repositories are configured in :attr:`audb.config.REPOSITORIES`
and can be managed best
by specifying them in the :ref:`configuration`.


Publish
-------

When publishing your data
with :func:`audb.publish`
the following operations are performed:

1. calculate :ref:`database dependencies <database-dependencies>`
2. pack the files into ZIP archives
3. upload all files to the backend

.. graphviz:: pics/publish.dot


Load
----

In the process of loading data
with :func:`audb.load`
the following operations are performed:

1. find the backends where the database is stored
2. find the latest version of a database (optional)
3. calculate :ref:`database dependencies <database-dependencies>`
4. download archive files from the selected backend
5. unpack the archive files
6. inspect and :ref:`convert <media-conversion-and-flavors>`
   the audio files (optional)
7. store the data in a :ref:`cache <caching>` folder

.. graphviz:: pics/load.dot


.. _Generic repository: https://www.jfrog.com/confluence/display/JFROG/Repository+Management#RepositoryManagement-GenericRepositories
.. _Artifactory server: https://jfrog.com/artifactory/
.. _implements the required functions: https://github.com/audeering/audbackend/blob/edd23462799ae9052a43cdd045698f78e19dbcaf/audbackend/core/backend.py#L559-L659

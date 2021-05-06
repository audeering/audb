.. Import audb
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb


.. _caching:

Caching
=======

When you request a database the first time,
:mod:`audb` will unpack (and convert) annotations and data to
:file:`<cache>/<name>/<version>/<flavor>/`.
Next time your request it again,
:mod:`audb` will directly load the database from there.

:mod:`audb` distinguishes two :file:`<cache>` folders.
A system wide shared cache folder and a user cache folder.


Shared cache
------------

By default the shared cache is located at

.. jupyter-execute::

    audb.default_cache_root(shared=True)

and is meant to be shared by several users.

In order to provide read and write access
to all users to the shared cache folder,
they need to be in the same group
and you have to `adjust the rights`_
of the shared cache folder with

.. code-block:: bash

    $ chmod 775 /data/audb  # set right of cache folder
    $ chmod g+s /data/audb  # content inherits group ownership
    $ setfacl -d -m u::rwx,g::rwx,o::rx- /data/audb  # content inherits rights


User cache
----------

The second cache folder should be
accessible to you only.
By default it points to

.. jupyter-execute::

    audb.default_cache_root(shared=False)

When you request a database with :meth:`audb.load`,
:mod:`audb` first looks for it in the shared cache folder
and afterwards in your local cache folder.


Changing cache locations
------------------------

There are four ways to change the default locations:

1. By setting the argument ``cache_root`` during a function call, e.g.

.. code-block:: python

    audb.load('emodb', ..., cache_root='/cache/root/audb')

2. System-wide by setting the following system variables

.. code-block:: bash

    export AUDB_CACHE_ROOT=/new/local/cache/audb
    export AUDB_SHARED_CACHE_ROOT=/new/shared/cache/audb

3. Program-wide by overwriting the default values in :class:`audb.config`

.. jupyter-execute::

    audb.config.SHARED_CACHE_ROOT = '/new/shared/cache/audb'
    audb.default_cache_root(shared=True)

.. jupyter-execute::

    audb.config.CACHE_ROOT = '/new/local/cache/audb'
    audb.default_cache_root(shared=False)

4. System wide by
   using the :ref:`configuration file <configuration>`
   :file:`~/.audb.yaml`

Note,
1. overwrites all other methods,
2. overwrites 3. and 4.,
and so on.


.. _adjust the rights: https://superuser.com/a/264406

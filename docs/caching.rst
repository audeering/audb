.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb2


.. _caching:

Caching
=======

When you request a database the first time,
:mod:`audb2` will unpack (and convert) annotations and data to
:file:`<cache>/<repository>/<group_id>/<name>/<flavor>/<version>/`.
Next time your request it again,
:mod:`audb2` will directly load the database from there.

:mod:`audb2` distinguishes two :file:`<cache>` folders.
A system wide shared cache folder.
By default it is located at

.. jupyter-execute::

    audb2.default_cache_root(shared=True)

and is meant to be shared by several users.

Please use the shared folder only
for all databases with public access level.

The second cache folder should be
accessible to you only.
It is reserved for databases that
are not relevant to other users or
databases that have private access level.
By default it points to

.. jupyter-execute::

    audb2.default_cache_root(shared=False)

When you request a database with :meth:`audb2.load`,
:mod:`audb2` first looks for it in the shared cache folder
and afterwards in your local cache folder.

There are four ways to change the default locations:

1. By setting the argument ``cache_root`` during a function call, e.g:

.. code-block:: python

    audb2.load('emodb', ..., cache_root='/cache/root/audb2')

2. System-wide by setting the following system variables:

.. code-block:: bash

    export AUDB2_CACHE_ROOT=/new/local/cache/audb2
    export AUDB2_SHARED_CACHE_ROOT=/new/shared/cache/audb2

3. Program-wide by overwriting the default values in :class:`audb2.config`:

.. jupyter-execute::

    audb2.config.SHARED_CACHE_ROOT = '/new/shared/cache/audb2'
    audb2.default_cache_root(shared=True)

.. jupyter-execute::

    audb2.config.CACHE_ROOT = '/new/local/cache/audb2'
    audb2.default_cache_root(shared=False)

4. System wide by
   using the :ref:`configuration file <configuration>`
   :file:`~/.audb.yaml`

Note,
1. overwrites all other methods,
2. overwrites 3. and 4.,
and so on.

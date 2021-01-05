.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb2


.. _cache-root:

Caching
=======

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
By default it points to :file:`~/audb2`.

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


.. _available databases:
    http://data.pp.audeering.com/databases.html
.. _compute servers:
    https://gitlab.audeering.com/devops/computex

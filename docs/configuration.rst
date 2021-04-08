.. Import audb
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb


.. _configuration:

Configuration
-------------

:mod:`audb` can be configured with a :file:`~/.audb.yaml` file.
The configuration file is read during import
and will overwrite the default settings.
The default settings are:

.. literalinclude:: ../audb/core/etc/audb.yaml
    :language: yaml

After loading :mod:`audb`
they can be accessed
or changed
using :class:`audb.config`.

.. jupyter-execute::

    audb.config.CACHE_ROOT

.. jupyter-execute::

    audb.config.SHARED_CACHE_ROOT

.. jupyter-execute::

    audb.config.REPOSITORIES

.. jupyter-execute::

    audb.config.CACHE_ROOT = '/user/cache'
    audb.config.CACHE_ROOT

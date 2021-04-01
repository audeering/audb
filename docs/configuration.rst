.. Import audb2
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb2


.. _configuration:

Configuration
-------------

:mod:`audb2` can be configured with a :file:`~/.audb.yaml` file.
The configuration file is read during import
and will overwrite the default settings.
The default settings are:

.. literalinclude:: ../audb2/core/etc/audb.yaml
    :language: yaml

After loading :mod:`audb2`
they can be accessed
or changed
using :class:`audb2.config`:

.. jupyter-execute::

    audb2.config.CACHE_ROOT

.. jupyter-execute::

    audb2.config.SHARED_CACHE_ROOT

.. jupyter-execute::

    audb2.config.REPOSITORIES

.. jupyter-execute::

    audb2.config.CACHE_ROOT = '/user/cache'
    audb2.config.CACHE_ROOT

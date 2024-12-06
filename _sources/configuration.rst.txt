.. _configuration:

Configuration
-------------

:mod:`audb` can be configured with a :file:`~/.config/audb.yaml` file.
The configuration file is read during import
and will overwrite the default settings.
The default settings are:

.. literalinclude:: ../audb/core/etc/audb.yaml
    :language: yaml

After loading :mod:`audb`
they can be accessed
or changed
using :class:`audb.config`.

>>> audb.config.CACHE_ROOT
'~/audb'

>>> audb.config.SHARED_CACHE_ROOT
'/data/audb'

>>> audb.config.REPOSITORIES
[Repository('audb-public', 's3.dualstack.eu-north-1.amazonaws.com', 's3')]

>>> audb.config.CACHE_ROOT = "/user/cache"
>>> audb.config.CACHE_ROOT
'/user/cache'

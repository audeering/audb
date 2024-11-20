..
    >>> import audb

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

>>> audb.config.CACHE_ROOT
'~/audb'

>>> audb.config.SHARED_CACHE_ROOT
'/data/audb'

>>> audb.config.REPOSITORIES
[Repository('data-public', 'https://audeering.jfrog.io/artifactory', 'artifactory'),
 Repository('data-local', '~/audb-host', 'file-system')]

>>> audb.config.CACHE_ROOT = "/user/cache"
>>> audb.config.CACHE_ROOT
'/user/cache'

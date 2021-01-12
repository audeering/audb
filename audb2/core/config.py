class config:
    """Get/set defaults for the :mod:`audb2` module.

    For example, when you want to change the default cache folder::

        import audb2
        audb2.config.CACHE_ROOT = '~/data'

    """

    ARTIFACTORY_HOST = 'https://artifactory.audeering.com/artifactory'
    r"""Default Artifactory host URL."""

    CACHE_ROOT = '~/audb2'
    r"""Default cache folder."""

    FILE_SYSTEM_HOST = '~/audb2-host'
    r"""Default file system host folder."""

    GROUP_ID = 'com.audeering.data'
    r"""Default group ID."""

    REPOSITORIES = ['data-public-local']
    r"""List of repositories, will be iterated in given order."""

    SHARED_CACHE_ROOT = '/data/audb2'
    r"""Default shared cache folder.

    This will be checked for data loading
    before :attr:`audb2.config.CACHE_ROOT`.

    """

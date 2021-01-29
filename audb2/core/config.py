class config:
    """Get/set defaults for the :mod:`audb2` module.

    For example, when you want to change the default cache folder::

        import audb2
        audb2.config.CACHE_ROOT = '~/data'

    """

    ARTIFACTORY_HOST = 'https://artifactory.audeering.com/artifactory'
    r"""Default Artifactory host URL."""

    ARTIFACTORY_REGISTRY_NAME = 'artifactory'
    r"""Name of Artifactory backend."""

    CACHE_ROOT = '~/audb2'
    r"""Default cache folder."""

    FILE_SYSTEM_HOST = '~/audb2-host'
    r"""Default file system host folder."""

    FILE_SYSTEM_REGISTRY_NAME = 'file-system'
    r"""Default file system host folder."""

    GROUP_ID = 'com.audeering.data'
    r"""Default group ID."""

    REPOSITORIES = [
        (
            ARTIFACTORY_REGISTRY_NAME,
            ARTIFACTORY_HOST,
            'data-public-local',
        ),
        (
            ARTIFACTORY_REGISTRY_NAME,
            ARTIFACTORY_HOST,
            'data-private-local',
        ),
        (
            FILE_SYSTEM_REGISTRY_NAME,
            FILE_SYSTEM_HOST,
            'data-local',
        ),
    ]
    r"""List of repositories, will be iterated in given order.
    
    Defines by a tuple with three entries:
    
    * the name of the backend
    * the host address
    * the name of the repository
    
    """

    SHARED_CACHE_ROOT = '/data/audb2'
    r"""Default shared cache folder.

    This will be checked for data loading
    before :attr:`audb2.config.CACHE_ROOT`.

    """

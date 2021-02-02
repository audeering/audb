class config:
    """Get/set defaults for the :mod:`audb2` module.

    For example, when you want to change the default cache folder::

        import audb2
        audb2.config.CACHE_ROOT = '~/data'

    """
    CACHE_ROOT = '~/audb2'
    r"""Default cache folder."""

    GROUP_ID = 'com.audeering.data'
    r"""Default group ID."""

    REPOSITORIES = [
        {
            'name': 'data-public-local',
            'backend': 'artifactory',
            'host': 'https://artifactory.audeering.com/artifactory',
        },
        {
            'name': 'data-private-local',
            'backend': 'artifactory',
            'host': 'https://artifactory.audeering.com/artifactory',

        },
        {
            'name': 'data-local',
            'backend': 'file-system',
            'host': '~/audb2-host',
        },
    ]
    r"""List of repositories, will be iterated in given order.

    A repository is defined by a dictionary with three entries:

    * 'name': repository name, e.g. 'data-public-local'
    * 'backend': backend name, e.g. 'artifactory'
    * 'host': host name, e.g. 'https://artifactory.audeering.com/artifactory'

    """

    REPOSITORY_PUBLISH = {
        'name': 'data-public-local',
        'backend': 'artifactory',
        'host': 'https://artifactory.audeering.com/artifactory',
    }
    r"""Default repository for publishing databases."""

    SHARED_CACHE_ROOT = '/data/audb2'
    r"""Default shared cache folder.

    This will be checked for data loading
    before :attr:`audb2.config.CACHE_ROOT`.

    """

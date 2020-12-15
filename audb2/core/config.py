class config:
    """Get/set defaults for the :mod:`audb2` module.

    For example, when you want to change the default cache folder::

        import audb2
        audb2.config.CACHE_ROOT = '~/data'

    """

    CACHE_ROOT = '~/audb2'
    """Cache folder."""

    GROUP_ID = 'com.audeering.data'
    """Default group ID."""

    REPOSITORY_PRIVATE = 'data-private-local'
    r"""Default private repository."""

    REPOSITORY_PUBLIC = 'data-public-local'
    r"""Default public repository."""

    SHARED_CACHE_ROOT = '/data/audb2'
    """Shared cache folder.

    This will be checked for data loading
    before :attr:`audb2.config.CACHE_ROOT`.

    """

import os

import oyaml as yaml

import audeer

from audb.core.define import (
    CONFIG_FILE,
    USER_CONFIG_FILE,
)
from audb.core.repository import Repository


def load_configuration_file(config_file: str):
    r"""Read configuration from YAML file.

    Args:
        config_file: path to configuration file.
            File doesn't have to exist

    Returns:
        dictionary containing configuration entries

    Raises:
        ValueError: if ``repositories`` section is present,
            but empty
        ValueError: if repository has no ``host``
            ``backend``, or ``name`` key

    """
    if os.path.exists(config_file):
        with open(config_file, 'r') as cf:
            config = yaml.load(cf, Loader=yaml.BaseLoader)
    else:
        config = {}

    # Check that we have provided a valid repositories configuration
    if 'repositories' in config:
        if len(config['repositories']) == 0:
            raise ValueError(
                "You cannot specify an empty 'repositories:' section "
                f"in the configuration file '{USER_CONFIG_FILE}'."
            )
        for n, repo in enumerate(config['repositories']):
            if 'host' not in repo:
                raise ValueError(
                    f"Your repository is missing a 'host' entry: '{repo}'."
                )
            if 'backend' not in repo:
                raise ValueError(
                    f"Your repository is missing a 'backend' entry: '{repo}'."
                )
            if 'name' not in repo:
                raise ValueError(
                    f"Your repository is missing a 'name' entry: '{repo}'."
                )

    return config


# Read in configuration from global and user file
root = os.path.dirname(os.path.realpath(__file__))
global_config_file = os.path.join(root, CONFIG_FILE)
user_config_file = audeer.safe_path(USER_CONFIG_FILE)
global_config = load_configuration_file(global_config_file)
user_config = load_configuration_file(user_config_file)
global_config.update(user_config)


class config:
    """Get/set configuration values for the :mod:`audb` module.

    The configuration values are read in during module import
    from the :ref:`configuration file <configuration>`
    :file:`~/.audb.yaml`.

    You can change the configuration values after import,
    by setting the attributes directly.

    The :ref:`caching <caching>` related configuration values
    can be overwritten by environment variables.

    Example:

        >>> config.CACHE_ROOT
        '~/audb'
        >>> config.CACHE_ROOT = '~/caches/audb'
        >>> config.CACHE_ROOT
        '~/caches/audb'

    """

    CACHE_ROOT = global_config['cache_root']
    r"""Default user cache folder."""

    REPOSITORIES = [
        Repository(r['name'], r['host'], r['backend'])
        for r in global_config['repositories']
    ]
    r"""Repositories, will be iterated in given order.

    A repository is defined by the object :class:`audb.Repository`,
    containing the following attributes:

    * :attr:`audb.Repository.name`: repository name, e.g. ``'data-local'``
    * :attr:`aub2.Repository.backend`: backend name, e.g. ``'artifactory'``
    * :attr:`audb.Repository.host`: host name,
      e.g. ``'https://artifactory.audeering.com/artifactory'``

    """

    SHARED_CACHE_ROOT = global_config['shared_cache_root']
    r"""Shared cache folder."""

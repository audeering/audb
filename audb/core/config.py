import os

import oyaml as yaml

import audeer

from audb.core.define import CONFIG_FILE
from audb.core.define import DEPRECATED_USER_CONFIG_FILE
from audb.core.define import USER_CONFIG_FILE
from audb.core.repository import Repository


CWD = audeer.script_dir()
global_config_file = os.path.join(CWD, CONFIG_FILE)


def load_configuration_file(config_file: str) -> dict:
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
    if not os.path.exists(config_file):
        return {}

    with open(config_file) as cf:
        config = yaml.load(cf, Loader=yaml.BaseLoader)
        if config is None:
            return {}

    # Check that we have provided a valid repositories configuration
    if "repositories" in config:
        if len(config["repositories"]) == 0:
            raise ValueError(
                "You cannot specify an empty 'repositories:' section "
                f"in the configuration file '{USER_CONFIG_FILE}'."
            )
        for n, repo in enumerate(config["repositories"]):
            if "host" not in repo:
                raise ValueError(
                    f"Your repository is missing a 'host' entry: '{repo}'."
                )
            if "backend" not in repo:
                raise ValueError(
                    f"Your repository is missing a 'backend' entry: '{repo}'."
                )
            if "name" not in repo:
                raise ValueError(
                    f"Your repository is missing a 'name' entry: '{repo}'."
                )

    return config


def load_config() -> dict:
    """Read configuration from configuration files.

    User config values take precedence over global config values
    when the same setting exists in both files.

    """
    # Global config
    config = load_configuration_file(global_config_file)
    # User config
    if os.path.exists(audeer.path(USER_CONFIG_FILE)):
        user_config_file = audeer.path(USER_CONFIG_FILE)
    else:
        user_config_file = audeer.path(DEPRECATED_USER_CONFIG_FILE)
    user_config = load_configuration_file(user_config_file)
    config.update(user_config)
    return config


class config:
    """Get/set configuration values for the :mod:`audb` module.

    The configuration values are read in during module import
    from the :ref:`configuration file <configuration>`
    :file:`~/.config/audb.yaml`.
    You can change the configuration values after import,
    by setting the attributes directly.
    The :ref:`caching <caching>` related configuration values
    can be overwritten by environment variables.

    Examples:
        >>> audb.config.CACHE_ROOT
        '~/audb'
        >>> audb.config.CACHE_ROOT = "~/caches/audb"
        >>> audb.config.CACHE_ROOT
        '~/caches/audb'

    """

    _config = load_config()

    CACHE_ROOT = _config["cache_root"]
    r"""Default user cache folder."""

    REPOSITORIES = [
        Repository(r["name"], r["host"], r["backend"]) for r in _config["repositories"]
    ]
    r"""Repositories, will be iterated in given order.

    A repository is defined by the object :class:`audb.Repository`,
    containing the following attributes:

    * :attr:`audb.Repository.name`: repository name, e.g. ``"audb-public"``
    * :attr:`audb.Repository.backend`: backend name, e.g. ``"s3"``
    * :attr:`audb.Repository.host`: host name,
      e.g. ``"s3.dualstack.eu-north-1.amazonaws.com"``

    """

    SHARED_CACHE_ROOT = _config["shared_cache_root"]
    r"""Shared cache folder."""

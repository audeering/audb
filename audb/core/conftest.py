from doctest import ELLIPSIS
from doctest import NORMALIZE_WHITESPACE
import os

import pytest
import sybil
from sybil.parsers.rest import DocTestParser

import audb
from audb.core.config import global_config_file
from audb.core.config import load_configuration_file


def imports(namespace):
    """Provide Python modules to namespace."""
    namespace["audb"] = audb


@pytest.fixture(scope="module", autouse=True)
def cache(tmpdir_factory):
    r"""Provide a reusable cache for docstring tests.

    As we rely on emodb from the public repo,
    it makes sense to cache it
    across all docstring tests.

    We use a environment variables,
    and set ``audb.config.CACHE_ROOT``
    and ``audb.config.SHARED_CACHE_ROOT``
    to its default values.

    """
    cache = tmpdir_factory.mktemp("cache")
    # Store original values
    env_cache = os.environ.get("AUDB_CACHE_ROOT", None)
    env_shared_cache = os.environ.get("AUDB_SHARED_CACHE_ROOT", None)
    config_cache = audb.config.CACHE_ROOT
    config_shared_cache = audb.config.SHARED_CACHE_ROOT
    # Assign values for test
    os.environ["AUDB_CACHE_ROOT"] = str(cache)
    os.environ["AUDB_SHARED_CACHE_ROOT"] = str(cache)
    default_config = load_configuration_file(global_config_file)
    audb.config.CACHE_ROOT = default_config["cache_root"]
    audb.config.SHARED_CACHE_ROOT = default_config["shared_cache_root"]

    yield

    # Reassign original values
    if env_cache is None:
        del os.environ["AUDB_CACHE_ROOT"]
    else:  # pragma: nocover
        os.environ["AUDB_CACHE_ROOT"] = env_cache
    if env_shared_cache is None:
        del os.environ["AUDB_SHARED_CACHE_ROOT"]
    else:  # pragma: nocover
        os.environ["AUDB_SHARED_CACHE_ROOT"] = env_shared_cache
    audb.config.CACHE_ROOT = config_cache
    audb.config.SHARED_CACHE_ROOT = config_shared_cache


@pytest.fixture(autouse=True)
def public_repository():
    r"""Provide access to the public Artifactory repository."""
    audb.config.REPOSITORIES = [
        audb.Repository(
            name="audb-public",
            host="s3.dualstack.eu-north-1.amazonaws.com",
            backend="s3",
        ),
    ]

    yield

    # Remove public repo
    audb.config.REPOSITORIES.pop()


# Collect doctests
pytest_collect_file = sybil.Sybil(
    parsers=[DocTestParser(optionflags=ELLIPSIS + NORMALIZE_WHITESPACE)],
    patterns=["*.py"],
    fixtures=[
        "cache",
        "public_repository",
    ],
    setup=imports,
).pytest()

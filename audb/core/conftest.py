import os

import pytest

import audb


@pytest.fixture(scope="package", autouse=True)
def cache(tmpdir_factory):
    r"""Provide a reusable cache for docstring tests.

    As we rely on emodb from the public repo,
    it makes sense to cache it
    across all docstring tests.

    """
    cache = tmpdir_factory.mktemp("cache")
    # We use the environment variable here
    # to ensure audb.config.CACHE_ROOT
    # does still return the default config value
    # in the doctest
    env_cache = os.environ.get("AUDB_CACHE_ROOT", None)
    env_shared_cache = os.environ.get("AUDB_SHARED_CACHE_ROOT", None)
    os.environ["AUDB_CACHE_ROOT"] = str(cache)
    os.environ["AUDB_SHARED_CACHE_ROOT"] = str(cache)

    yield

    if env_cache is None:
        del os.environ["AUDB_CACHE_ROOT"]
    else:  # pragma: nocover
        os.environ["AUDB_CACHE_ROOT"] = env_cache

    if env_shared_cache is None:
        del os.environ["AUDB_SHARED_CACHE_ROOT"]
    else:  # pragma: nocover
        os.environ["AUDB_SHARED_CACHE_ROOT"] = env_shared_cache


@pytest.fixture(autouse=True)
def public_repository(doctest_namespace):
    r"""Provide access to the public Artifactory repository.

    Some tests in the docstrings need access to the emodb database.
    As all the unit tests defined under ``tests/*``
    should not be able to see the public repository
    as the number of available databases would then not be deterministic.
    We provide this access here
    with the help of the ``doctest_namespace`` fixture.

    The ``conftest.py`` file has to be in the same folder
    as the code file where the docstring is defined.

    """
    audb.config.REPOSITORIES = [
        audb.Repository(
            name="data-public",
            host="https://audeering.jfrog.io/artifactory",
            backend="artifactory",
        ),
    ]
    doctest_namespace["audb"] = audb

    yield

    # Remove public repo
    audb.config.REPOSITORIES.pop()

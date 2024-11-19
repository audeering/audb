from doctest import ELLIPSIS
from doctest import NORMALIZE_WHITESPACE
import os

import pytest
import sybil
from sybil.parsers.rest import DocTestParser

import audb


# Collect doctests
pytest_collect_file = sybil.Sybil(
    parsers=[DocTestParser(optionflags=ELLIPSIS + NORMALIZE_WHITESPACE)],
    patterns=["*.py"],
    fixtures=[
        "cache",
        "public_repository",
    ],
).pytest()


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
def public_repository():
    r"""Provide access to the public Artifactory repository."""
    audb.config.REPOSITORIES = [
        audb.Repository(
            name="data-public",
            host="https://audeering.jfrog.io/artifactory",
            backend="artifactory",
        ),
    ]

    yield audb

    # Remove public repo
    audb.config.REPOSITORIES.pop()

import os

import pytest

import audb


@pytest.fixture(scope='package', autouse=True)
def cache(tmp_path_factory):
    r"""Provide a reuseable cache for docstring tests.

    As we rely on emodb from the public repo,
    it makes sense to cache it
    across all docstring tests.

    """
    cache = tmp_path_factory.mktemp('cache').as_posix()
    os.environ['AUDB_CACHE_ROOT'] = cache
    return cache


@pytest.fixture(autouse=True)
def add_audb_with_public_data(doctest_namespace):
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
            name='data-public',
            host='https://audeering.jfrog.io/artifactory',
            backend='artifactory',
        ),
    ]
    doctest_namespace['audb'] = audb

    yield

    # Remove public repo
    audb.config.REPOSITORIES.pop()

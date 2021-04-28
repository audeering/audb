import pytest

import audb


@pytest.fixture(autouse=True)
def add_audb_with_public_data(doctest_namespace):
    r"""Provide access to the public Artifactory repository.

    Some tests in the docstrings need access to the emodb database.
    As all the unit tests defined under ``tests/*``
    should not be able to see the public repository
    as the number of available databases would then not be deterministic,
    we have to add the extra ``conftest.py`` file here instead.

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
    audb.config.REPOSITORIES = pytest.REPOSITORIES

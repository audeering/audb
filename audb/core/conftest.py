import pytest

import audb


@pytest.fixture(autouse=True)
def add_audb_with_public_data(doctest_namespace):
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

import os

import pytest

import audb
import audeer


def test_available():

    # Non existing repo
    name = 'non-existing-repo'
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=name,
            host=pytest.FILE_SYSTEM_HOST,
            backend=pytest.BACKEND,
        )
    ]
    df = audb.available()
    assert len(df) == 0

    # Borken database in repo
    name = 'non-existing-database'
    audb.config.REPOSITORIES = pytest.REPOSITORIES
    path = os.path.join(
        audb.config.REPOSITORIES[0].host,
        audb.config.REPOSITORIES[0].name,
        name,
    )
    path = audeer.mkdir(path)
    audb.available()
    os.rmdir(path)
    assert len(df) == 0

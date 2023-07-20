import os

import audeer

import audb


def test_available(repository):

    # Broken database in repo
    name = 'non-existing-database'
    path = os.path.join(
        repository.host,
        repository.name,
        name,
    )
    path = audeer.mkdir(path)
    df = audb.available()
    os.rmdir(path)
    assert len(df) == 0

    # Non existing repo
    name = 'non-existing-repo'
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=name,
            host=repository.host,
            backend=repository.backend,
        )
    ]
    df = audb.available()
    assert len(df) == 0

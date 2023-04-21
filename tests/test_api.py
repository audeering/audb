import os

import audb
import audeer


def test_available(repository):

    # Non existing repo
    name = 'non-existing-repo'
    df = audb.available()
    assert len(df) == 0

    # Broken database in repo
    name = 'non-existing-database'
    path = os.path.join(
        repository.host,
        repository.name,
        name,
    )
    path = audeer.mkdir(path)
    audb.available()
    os.rmdir(path)
    assert len(df) == 0

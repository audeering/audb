import os

import audeer
import audformat

import audb


def test_available(repository):
    # Broken database in repo
    name = "non-existing-database"
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
    name = "non-existing-repo"
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=name,
            host=repository.host,
            backend=repository.backend,
        )
    ]
    df = audb.available()
    assert len(df) == 0


def test_available_broken_dataset(private_and_public_repository):
    """Test for listing datasets, including a broken one.

    This uses the public repositories,
    from which ``data-public2``
    includes ``broken-dataset``,
    which has a missing ``db`` folder.

    """
    df = audb.available(only_latest=True)
    assert len(df) > 0
    assert "broken-dataset" not in df


def test_versions(tmpdir, repository):
    """Test versions() for non existing repositories.

    As ``audb.versions()`` does not use ``audb.core.utils._lookup()``
    to get the backend of the corresponding database,
    but goes through all provided backends to collect versions
    of the requested database,
    we need to ensure,
    that it does not crash
    when a repository does not exist.

    See https://github.com/audeering/audb/issues/389.

    """
    # Add non existing repository to the list of configured repositories
    non_existing_repository = audb.Repository(
        name="non-existing-repo",
        host="non-existing-host",
        backend="file-system",
    )
    audb.config.REPOSITORIES += [non_existing_repository]
    # Publish a dataset to an existing repository
    name = "mydb"
    version = "1.0.0"
    build_dir = audeer.mkdir(tmpdir, "build")
    db = audformat.Database(name)
    db.save(build_dir)
    audb.publish(build_dir, version, repository)

    assert audb.versions(name) == [version]

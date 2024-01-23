import audb


def test_visiting_private_repos(private_and_public_repository):
    r"""Tests visiting private repos when looking for a database.

    When requesting a database,
    audb needs to look for it
    in every repository on the corresponding backend.
    This should not fail,
    even when the user has no access rights.

    """
    audb.load(
        "emodb",
        version="1.4.1",
        only_metadata=True,
        verbose=False,
    )


def test_visiting_non_existing_repos(non_existing_repository):
    r"""Tests visiting non-existing backends when looking for a database.

    When requesting a database,
    audb needs to look for it
    in every repository on the corresponding backend.
    This should not fail,
    even when a repository does not exist.

    """
    audb.load(
        "emodb",
        version="1.4.1",
        only_metadata=True,
        verbose=False,
    )

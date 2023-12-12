import pytest

import audb


@pytest.mark.parametrize(
    'repos',
    [
        'public_and_empty_repository',
        'empty_and_public_repository',
    ],
)
def test_visiting_multiple_repos(request, repos):
    r"""Tests visiting multiple repos when looking for a database.

    When requesting a database,
    audb needs to look for it
    in every repository on the corresponding backend.

    """
    request.getfixturevalue(repos)
    db = audb.load(
        'emodb',
        version='1.4.1',
        only_metadata=True,
        verbose=False,
    )
    assert db.name == 'emodb'
    df = audb.available(only_latest=True)
    assert 'emodb' in df.index
    deps = audb.dependencies('emodb', version='1.4.1')
    assert 'wav/13b09La.wav' in deps.media


@pytest.mark.parametrize(
    'repos',
    [
        'public_and_private_repository',
        'private_and_public_repository',
    ],
)
def test_visiting_private_repos(request, repos):
    r"""Tests visiting private repos when looking for a database.

    When requesting a database,
    audb needs to look for it
    in every repository on the corresponding backend.
    This should not fail,
    even when the user has no access rights.

    """
    request.getfixturevalue(repos)
    db = audb.load(
        'emodb',
        version='1.4.1',
        only_metadata=True,
        verbose=False,
    )
    assert db.name == 'emodb'
    # The following fails,
    # see https://github.com/audeering/audb/issues/340
    # df = audb.available(only_latest=True)
    # assert 'emodb' in df.index
    # deps = audb.dependencies('emodb', version='1.4.1')
    # assert 'wav/13b09La.wav' in deps.media


def test_visiting_non_existing_repos(non_existing_repository):
    r"""Tests visiting non-existing backends when looking for a database.

    When requesting a database,
    audb needs to look for it
    in every repository on the corresponding backend.
    This should not fail,
    even when a repository does not exist.

    """
    audb.load(
        'emodb',
        version='1.4.1',
        only_metadata=True,
        verbose=False,
    )

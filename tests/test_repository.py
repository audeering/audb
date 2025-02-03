import sys

import pytest

import audbackend

import audb


if hasattr(audbackend.backend, "Artifactory"):
    artifactory_backend = audbackend.backend.Artifactory
else:
    artifactory_backend = None


@pytest.mark.parametrize(
    "repository1, repository2, expected",
    [
        (
            audb.Repository("repo", "host", "file-system"),
            audb.Repository("repo", "host", "file-system"),
            True,
        ),
        (
            audb.Repository("repo1", "host", "file-system"),
            audb.Repository("repo2", "host", "file-system"),
            False,
        ),
    ],
)
def test_repository_eq(repository1, repository2, expected):
    """Test the Repository.__eq__().

    Two repository instances are equal,
    if their string representation matches.

    Args:
        repository1: repository instance
        repository2: repository instance
        expected: expected return value of ``Repository.__eq__()``

    """
    assert (repository1 == repository2) == expected


@pytest.mark.parametrize(
    "repo1, repo2, should_have_same_hash",
    [
        # Same repositories should have same hash
        (
            audb.Repository("repo", "host", "file-system"),
            audb.Repository("repo", "host", "file-system"),
            True,
        ),
        # Different attributes should have different hashes
        (
            audb.Repository("repo1", "host", "file-system"),
            audb.Repository("repo2", "host", "file-system"),
            False,
        ),
        (
            audb.Repository("repo", "host1", "file-system"),
            audb.Repository("repo", "host2", "file-system"),
            False,
        ),
        (
            audb.Repository("repo", "host", "file-system"),
            audb.Repository("repo", "host", "s3"),
            False,
        ),
    ],
)
def test_repository_hash(repo1, repo2, should_have_same_hash):
    """Test Repository object hash behavior.

    Tests that:
    - Repository objects are hashable
    - Equal repositories have same hash
    - Different repositories have different hashes

    Args:
        repo1: repository object
        repo2: repository object
        should_have_same_hash: if ``True``,
            expects ``repo1`` and ``repo2``
            to be the same object

    """
    # Verify objects are hashable
    assert isinstance(hash(repo1), int)
    assert isinstance(hash(repo2), int)

    # Test hash equality matches object equality
    if should_have_same_hash:
        assert hash(repo1) == hash(repo2)
        assert repo1 == repo2
    else:
        assert hash(repo1) != hash(repo2)
        assert repo1 != repo2

    # Verify can be used in sets
    test_set = {repo1, repo2}
    expected_len = 1 if should_have_same_hash else 2
    assert len(test_set) == expected_len


@pytest.mark.parametrize(
    "backend, host, repo, expected",
    [
        (
            "file-system",
            "host",
            "repo",
            "Repository('repo', 'host', 'file-system')",
        ),
    ],
)
def test_repository_repr(backend, host, repo, expected):
    """Test string representation of Repository.

    Args:
        backend: backend name
        host: host
        repo: repository name
        expected: expected string representation

    """
    repository = audb.Repository(repo, host, backend)
    assert str(repository) == expected


@pytest.mark.parametrize(
    "backend, host, repo, expected_backend, expected_interface",
    [
        (
            "file-system",
            "host",
            "repo",
            audbackend.backend.FileSystem,
            audbackend.interface.Versioned,
        ),
        pytest.param(
            "artifactory",
            "host",
            "repo",
            artifactory_backend,
            audbackend.interface.Maven,
            marks=pytest.mark.skipif(
                sys.version_info >= (3, 13),
                reason="No artifactory backend support in Python>=3.13",
            ),
        ),
    ],
)
def test_repository_create_backend_interface(
    backend,
    host,
    repo,
    expected_backend,
    expected_interface,
):
    """Test creation of backend interface.

    Args:
        backend: backend name
        host: host
        repo: repository name
        expected_backend: expected backend class
        expected_interface: expected backend interface class

    """
    repository = audb.Repository(repo, host, backend)
    backend_interface = repository.create_backend_interface()
    assert isinstance(backend_interface, expected_interface)
    assert isinstance(backend_interface.backend, expected_backend)
    assert not backend_interface.backend.opened


@pytest.mark.parametrize(
    "backend, host, repo, expected_error_msg, expected_error",
    [
        (
            "custom",
            "host",
            "repo",
            "'custom' is not a registered backend",
            ValueError,
        ),
        pytest.param(
            "artifactory",
            "host",
            "repo",
            "The 'artifactory' backend is not supported in Python>=3.13",
            ValueError,
            marks=pytest.mark.skipif(
                sys.version_info < (3, 13),
                reason="Should only fail for Python>=3.13",
            ),
        ),
    ],
)
def test_repository_create_backend_interface_errors(
    backend,
    host,
    repo,
    expected_error_msg,
    expected_error,
):
    repository = audb.Repository(repo, host, backend)
    with pytest.raises(expected_error, match=expected_error_msg):
        repository.create_backend_interface()

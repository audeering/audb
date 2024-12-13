import pytest

import audeer
import audformat

import audb


class TestAvailable:
    r"""Test collecting available datasets."""

    @classmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup(cls, tmpdir_factory):
        r"""Prepare repositories and datasets.

        Creates two repositories
        with different hosts, repositories and datasets:

        * ``<tmpdir>/host0/repo0/name0/1.0.0/db.yaml``
        * ``<tmpdir>/host1/repo1/name1/1.0.0/db.yaml``

        Those two repositories are then set as default for ``audb``.

        Args:
            tmpdir_factory: tmpdir_factory fixture

        """
        current_repositories = audb.config.REPOSITORIES
        # First repository
        host0 = str(tmpdir_factory.mktemp("host0"))
        audeer.touch(audeer.mkdir(host0, "repo0", "name0", "1.0.0"), "db.yaml")
        # Second repository
        host1 = str(tmpdir_factory.mktemp("host1"))
        audeer.touch(audeer.mkdir(host1, "repo1", "name1", "1.0.0"), "db.yaml")
        audb.config.REPOSITORIES = [
            audb.Repository("repo0", host0, "file-system"),
            audb.Repository("repo1", host1, "file-system"),
        ]
        yield
        audb.config.REPOSITORIES = current_repositories

    @pytest.fixture(scope="function", autouse=False)
    def repository_with_broken_database(self):
        """Create repository with empty folder.

        Adds an empty folder to the first repository.

        """
        repository = audb.config.REPOSITORIES[0]
        empty_folder = audeer.mkdir(repository.host, repository.name, "no-database")
        yield
        audeer.rmdir(empty_folder)

    @pytest.fixture(scope="function", autouse=False)
    def non_existing_repository(self):
        """Add non-existing repository to config."""
        repository = audb.config.REPOSITORIES[0]
        audb.config.REPOSITORIES.append(
            audb.Repository("non-existing", repository.host, "file-system")
        )
        yield
        audb.config.REPOSITORIES = audb.config.REPOSITORIES[:-1]

    @pytest.fixture(scope="function", autouse=False)
    def non_existing_artifactory_host(self):
        """Add non-existing host on Artifactory backend.

        This should also not fail under Python 3.12,
        which no longher supports Artifactory.

        """
        audb.config.REPOSITORIES.append(
            audb.Repository("repo", "https:artifactory.url.com", "artifactory")
        )
        yield
        audb.config.REPOSITORIES = audb.config.REPOSITORIES[:-1]

    @pytest.fixture(scope="function", autouse=False)
    def additional_version(self):
        """Publish a second version of first database."""
        repository = audb.config.REPOSITORIES[0]
        version2 = audeer.path(repository.host, "repo0", "name0", "2.0.0")
        audeer.touch(audeer.mkdir(version2), "db.yaml")
        yield
        audeer.rmdir(version2)

    def test_default(self):
        """Test available datasets with default arguments."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index

    def test_broken_database(self, repository_with_broken_database):
        """Test having a database only given as a folder."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index
        assert "no-database" not in df.index

    def test_non_existing_repository(self, non_existing_repository):
        """Test having a non-existing repoisitory in config."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index

    def test_non_existing_artifactory_host(self, non_existing_artifactory_host):
        """Test having a non-existing host on an Artifactory backend."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index

    def test_latest_version(self, additional_version):
        """Test only_latest argument."""
        df = audb.available()
        assert len(df) == 3
        assert "name0" in df.index
        assert "name1" in df.index
        assert len(df.loc["name0"]) == 2
        df = audb.available(only_latest=True)
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index
        assert df.loc["name0", "version"] == "2.0.0"
        assert df.loc["name1", "version"] == "1.0.0"


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

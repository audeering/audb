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

        Create two repositories
        with different hosts, repositories and datasets:

        * ``<tmpdir>/host0/repo0/name0/1.0.0/db.yaml``
        * ``<tmpdir>/host1/repo1/name1/1.0.0/db.yaml``

        The repositories are then set in ``audb.config.REPOSITORIES``.

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

        Adds an empty folder ``"no-database"``
        to the first repository.

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
        """Test available databases with default arguments."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index

    def test_repositories_all(self):
        """Test repositories argument with all repositories."""
        repositories = audb.config.REPOSITORIES
        df = audb.available(repositories=repositories)
        assert len(df) == 2

        # Verify repositories
        expected_repos = set([repo.name for repo in repositories])
        assert set(df.repository.unique()) == expected_repos

        # Verify database names
        assert "name0" in df.index
        assert "name1" in df.index

        # Verify hosts
        assert df.loc["name0", "host"] == repositories[0].host
        assert df.loc["name1", "host"] == repositories[1].host

    @pytest.mark.parametrize("repository_index", [0, 1])
    @pytest.mark.parametrize("preprocess_repository", [lambda x: x, lambda x: [x]])
    def test_repositories_single(self, repository_index, preprocess_repository):
        """Test repositories argument with single repositories.

        Args:
            repository_index: select single repository
                by the given index
                from ``audb.config.REPOSITORIES``
            preprocess_repository: apply given function
                to single repository
                before using as ``repositories`` argument

        """
        repository = audb.config.REPOSITORIES[repository_index]
        df = audb.available(repositories=preprocess_repository(repository))
        assert len(df) == 1

        # Verify repository
        assert df.repository.iloc[0] == repository.name

        # Verify database name
        assert df.index[0] == f"name{repository_index}"

        # Verify host
        assert df.host.iloc[0] == repository.host

    @pytest.mark.parametrize("repositories", [[], ()])
    def test_repositories_empty(self, repositories):
        """Tests empty repositories argument."""
        df = audb.available(repositories=repositories)
        assert len(df) == 0

    def test_broken_database(self, repository_with_broken_database):
        """Test having a database only given as a folder."""
        df = audb.available()
        assert len(df) == 2
        assert "name0" in df.index
        assert "name1" in df.index
        assert "no-database" not in df.index

    def test_non_existing_repository(self, non_existing_repository):
        """Test having a non-existing repository in config."""
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

    @pytest.mark.parametrize(
        "only_latest, expected_databases, expected_versions",
        [
            (True, ["name0", "name1"], ["2.0.0", "1.0.0"]),
            (False, ["name0", "name0", "name1"], ["1.0.0", "2.0.0", "1.0.0"]),
        ],
    )
    def test_latest_version(
        self,
        additional_version,
        only_latest,
        expected_databases,
        expected_versions,
    ):
        """Test only_latest argument.

        Args:
            additional_version: additional_version fixture
            only_latest: only_latest argument of audb.available()
            expected_databases: expected database names
                in index of dataframe
            expected_versions: expected database version
                in version column of dataframe

        """
        df = audb.available(only_latest=only_latest)
        assert len(df) == len(expected_databases)
        assert list(df.index) == expected_databases
        assert list(df["version"]) == expected_versions


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

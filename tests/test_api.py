import concurrent.futures

import pytest

import audeer
import audformat

import audb


class _FakePoolManager:
    r"""Mimic ``urllib3.PoolManager`` connection pool configuration."""

    def __init__(self):
        self.connection_pool_kw = {}


class _FakeClient:
    r"""Mimic ``minio.Minio`` holding an HTTP connection pool."""

    def __init__(self):
        self._http = _FakePoolManager()


class _FakeBackend:
    r"""Mimic a MinIO/S3 backend.

    Args:
        folders: mapping of database name
            to the list of folders stored under it,
            e.g. ``{"db": ["1.0.0", "media"]}``
        headers: mapping of database name
            to the list of versions
            for which a header file exists

    """

    def __init__(self, folders, headers):
        self.folders = folders
        self.headers = headers
        self._client = _FakeClient()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def ls_dirs(self, path):
        path = path.strip("/")
        if path == "":
            # Top level: database names
            return list(self.folders)
        # Sub level: folders under the database
        return self.folders[path]

    def exists(self, path):
        # ``path`` has the form ``/<name>/<version>/db.yaml``
        name, version = path.strip("/").split("/")[:2]
        return version in self.headers.get(name, [])


class _FakeInterface:
    r"""Mimic a backend interface wrapping a ``_FakeBackend``."""

    def __init__(self, backend):
        self.backend = backend


class TestAvailable:
    r"""Test collecting available datasets."""

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, tmpdir_factory):
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

    @pytest.fixture(scope="function", autouse=False)
    def minio_repository(self, monkeypatch):
        """Add an S3 repository served by a fake backend.

        The repository contains a single database ``"name-s3"`` with:

        * ``1.0.0`` and ``2.0.0`` -- proper versions with a header file
        * ``3.0.0`` -- version folder without a header file
        * ``media`` -- non-version folder

        ``audb.available()`` should only report
        ``1.0.0`` and ``2.0.0``.

        Yields:
            the fake backend instance,
            so its connection pool can be inspected

        """
        folders = {"name-s3": ["1.0.0", "2.0.0", "3.0.0", "media"]}
        headers = {"name-s3": ["1.0.0", "2.0.0"]}
        backend = _FakeBackend(folders, headers)

        original = audb.Repository.create_backend_interface

        def create_backend_interface(self):
            if self.backend == "s3":
                return _FakeInterface(backend)
            return original(self)

        monkeypatch.setattr(
            audb.Repository, "create_backend_interface", create_backend_interface
        )
        current_repositories = audb.config.REPOSITORIES
        audb.config.REPOSITORIES = current_repositories + [
            audb.Repository("repo-s3", "host-s3", "s3")
        ]
        yield backend
        audb.config.REPOSITORIES = current_repositories

    @pytest.mark.parametrize(
        "only_latest, expected_versions",
        [
            (False, ["1.0.0", "2.0.0"]),
            (True, ["2.0.0"]),
        ],
    )
    def test_minio_s3_backend(self, minio_repository, only_latest, expected_versions):
        """Test collecting versions from a MinIO/S3 backend.

        Args:
            minio_repository: minio_repository fixture
            only_latest: only_latest argument of audb.available()
            expected_versions: expected versions of ``"name-s3"``

        """
        num_workers = 4
        df = audb.available(only_latest=only_latest, num_workers=num_workers)
        assert "name-s3" in df.index
        versions = df.loc[["name-s3"], "version"].tolist()
        assert sorted(versions) == expected_versions
        # The HTTP connection pool size is matched to num_workers
        pool_kw = minio_repository._client._http.connection_pool_kw
        assert pool_kw["maxsize"] == num_workers

    @pytest.fixture(scope="function", autouse=False)
    def artifactory_repository(self, monkeypatch):
        """Add an Artifactory repository served by a fake backend.

        ``artifactory`` is not in the thread-safe allowlist,
        so ``audb.available()`` must access it
        with a single worker,
        regardless of ``num_workers``.
        Two databases are provided,
        so a missing single-worker limit
        would result in more than one worker.

        Yields:
            tuple of the fake backend instance and the repository

        """
        folders = {"db-a": ["1.0.0", "2.0.0"], "db-b": ["1.0.0"]}
        headers = {"db-a": ["1.0.0", "2.0.0"], "db-b": ["1.0.0"]}
        backend = _FakeBackend(folders, headers)
        repository = audb.Repository("repo-art", "host-art", "artifactory")

        def create_backend_interface(self):
            return _FakeInterface(backend)

        monkeypatch.setattr(
            audb.Repository, "create_backend_interface", create_backend_interface
        )
        yield backend, repository

    def test_non_thread_safe_backend_single_worker(
        self, artifactory_repository, monkeypatch
    ):
        """Test non-thread-safe backends are accessed with a single worker.

        Args:
            artifactory_repository: artifactory_repository fixture
            monkeypatch: monkeypatch fixture

        """
        backend, repository = artifactory_repository

        # Record the number of workers requested from the thread pool
        requested_workers = []
        executor = concurrent.futures.ThreadPoolExecutor

        def spy(max_workers, *args, **kwargs):
            requested_workers.append(max_workers)
            return executor(max_workers, *args, **kwargs)

        monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", spy)

        df = audb.available(num_workers=10, repositories=repository)

        # A single worker is used, even with num_workers=10 and two databases
        assert requested_workers == [1]
        # The connection pool is limited to a single connection as well
        assert backend._client._http.connection_pool_kw["maxsize"] == 1
        # Versions are still collected correctly
        assert sorted(df.index) == ["db-a", "db-a", "db-b"]


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


@pytest.mark.slow
def test_lazy_import():
    """Test that heavy dependencies are not imported with 'import audb'.

    With lazy loading, importing audb should not import pandas
    or other heavy dependencies until they are actually needed.

    This test uses subprocess to ensure a clean Python state,
    as modules remain in sys.modules once imported.

    """
    import subprocess
    import sys

    code = """
import sys
import audb

# Check 1: pandas should NOT be imported after 'import audb'
if 'pandas' in sys.modules:
    print('FAIL: pandas was imported on import audb')
    sys.exit(1)

# Check 2: pandas SHOULD be imported after accessing Dependencies
_ = audb.Dependencies
if 'pandas' not in sys.modules:
    print('FAIL: pandas should be imported after accessing Dependencies')
    sys.exit(1)

print('PASS')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Lazy import failed: {result.stdout}{result.stderr}"


def test_dir():
    """Test that dir() includes standard module attributes.

    With lazy loading, we need to ensure that standard module
    attributes are still available in dir().

    """
    # Standard module attributes
    standard_attrs = [
        "__all__",
        "__builtins__",
        "__cached__",
        "__doc__",
        "__file__",
        "__loader__",
        "__name__",
        "__package__",
        "__path__",
        "__spec__",
        "__version__",
    ]
    for attr in standard_attrs:
        assert attr in dir(audb), f"Missing standard attribute '{attr}'"

    # Test dir(audb.core) includes standard attributes and submodules
    submodule_standard_attrs = [
        "__builtins__",
        "__cached__",
        "__doc__",
        "__file__",
        "__loader__",
        "__name__",
        "__package__",
        "__path__",
        "__spec__",
    ]
    submodules = [audb.core, audb.info]
    for submodule in submodules:
        for attr in submodule_standard_attrs:
            err_msg = f"Missing standard attribute '{attr}' in submodule '{submodule}'"
            assert attr in dir(submodule), err_msg


def test_public_api_accessible():
    """Test that all public API symbols are accessible via lazy loading."""
    import types

    # Functions from audb.core.api
    api_functions = [
        "available",
        "cached",
        "dependencies",
        "exists",
        "flavor_path",
        "latest_version",
        "remove_media",
        "repository",
        "versions",
    ]
    for name in api_functions:
        attr = getattr(audb, name)
        assert callable(attr), f"audb.{name} should be callable"

    # Functions from other modules
    other_functions = [
        "default_cache_root",
        "load",
        "load_attachment",
        "load_media",
        "load_table",
        "load_to",
        "publish",
        "stream",
    ]
    for name in other_functions:
        attr = getattr(audb, name)
        assert callable(attr), f"audb.{name} should be callable"

    # Classes
    classes = ["Dependencies", "Flavor", "Repository", "DatabaseIterator"]
    for name in classes:
        attr = getattr(audb, name)
        assert isinstance(attr, type), f"audb.{name} should be a class"

    # Config object
    assert audb.config is not None

    # Submodules
    assert isinstance(audb.core, types.ModuleType)
    assert isinstance(audb.info, types.ModuleType)

    # Version
    assert isinstance(audb.__version__, str)

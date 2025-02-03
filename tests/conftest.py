import glob
import os
import platform
import sys

import pytest

import audeer

import audb


PUBLIC_HOST = "s3.dualstack.eu-north-1.amazonaws.com"
PUBLIC_BACKEND = "s3"


if platform.system() == "Darwin":
    # Avoid multi-threading on MacOS runner,
    # as it might fail from time to time
    # (memory issue?), see
    # https://github.com/audeering/audresample/issues/57
    pytest.NUM_WORKERS = 1
else:
    pytest.NUM_WORKERS = 5


@pytest.fixture(scope="package", autouse=True)
def cleanup_coverage_files():
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        ".coverage.*",
    )
    for file in glob.glob(path):
        os.remove(file)


# ===== CACHE =====
@pytest.fixture(scope="function", autouse=True)
def cache(tmpdir_factory):
    r"""Temp folder as cache.

    Provide a different temporary folder
    as audb cache root
    in each test.

    """
    cache = tmpdir_factory.mktemp("cache")
    current_cache = audb.config.CACHE_ROOT
    audb.config.CACHE_ROOT = str(cache)

    yield cache

    audb.config.CACHE_ROOT = current_cache


@pytest.fixture(scope="module", autouse=True)
def persistent_cache(tmpdir_factory):
    r"""Temp folder as module wide cache.

    Provide a different temporary folder
    as cache across all tests
    in a test definition file (module).

    This cache will be used automatically
    in all fixtures
    that have module as scope
    and access the cache folder.

    """
    cache = tmpdir_factory.mktemp("cache")
    current_cache = audb.config.CACHE_ROOT
    audb.config.CACHE_ROOT = str(cache)

    yield cache

    audb.config.CACHE_ROOT = current_cache


@pytest.fixture(scope="package", autouse=True)
def shared_cache(tmpdir_factory):
    r"""Temp folder as shared cache.

    Provide a single temporary folder
    as audb shared cache root
    across all tests.

    """
    cache = tmpdir_factory.mktemp("shared_cache")
    audb.config.SHARED_CACHE_ROOT = str(cache)
    return cache


@pytest.fixture(scope="package", autouse=True)
def hide_default_caches():
    r"""Hide default audb cache settings during testing.

    The cache and shared cache of audb
    are handled by ``audb.config.CACHE_ROOT``,
    ``audb.config.SHARED_CACHE_ROOT``
    and can in addition be configured
    by the environment variables
    ``AUDB_CACHE_ROOT``
    and ``AUDB_SHARED_CACHE_ROOT``.

    To ensure those will not interfere with the tests
    we hide them when executing the tests.

    """
    audb.config.CACHE_ROOT = None
    # Don't clean audb.config.SHARED_CACHE_ROOT
    # as it is set by shared_cache anyway

    env_cache = os.environ.get("AUDB_CACHE_ROOT", None)
    env_shared_cache = os.environ.get("AUDB_SHARED_CACHE_ROOT", None)
    if env_cache is not None:
        del os.environ["AUDB_CACHE_ROOT"]
    if env_shared_cache is not None:
        del os.environ["AUDB_SHARED_CACHE_ROOT"]

    yield

    if env_cache is not None:
        os.environ["AUDB_CACHE_ROOT"] = env_cache
    if env_shared_cache is not None:
        os.environ["AUDB_SHARED_CACHE_ROOT"] = env_shared_cache


# ===== REPOSITORIES =====
@pytest.fixture(scope="function", autouse=False)
def repository(tmpdir_factory):
    r"""Temp folder as repository.

    Provide a different temporary folder
    as repository in each test.
    This repository will be the only one visible
    inside the test.

    """
    host = tmpdir_factory.mktemp("host")
    name = "data-unittests-local"
    repository = audb.Repository(
        name=name,
        host=host,
        backend="file-system",
    )
    audeer.mkdir(host, name)
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [repository]

    yield repository

    audb.config.REPOSITORIES = current_repositories


@pytest.fixture(scope="module", autouse=False)
def persistent_repository(tmpdir_factory):
    r"""Temp folder as module wide repository.

    Provide a different temporary folder
    as repository across all tests
    in a test definition file (module).
    This repository will be the only one visible
    inside each test/fixture
    that uses it as argument.

    This fixture is useful to first publish
    one or more databases
    at the beginning of a test module
    (e.g. inside a fixture)
    and access those database(s)
    in different tests
    in the same module.

    """
    host = tmpdir_factory.mktemp("host")
    name = "data-unittests-local"
    repository = audb.Repository(
        name=name,
        host=host,
        backend="file-system",
    )
    audeer.mkdir(host, name)
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [repository]

    yield repository

    audb.config.REPOSITORIES = current_repositories


@pytest.fixture(scope="module", autouse=False)
def private_and_public_repository():
    r"""Private and public repositories.

    Configure the following repositories:

    * data-private: repo on public Artifactory without access
    * audb-private: repo on public S3 without access
    * data-public: repo on public Artifactory with anonymous access
    * audb-public: repo on public S3 with anonymous access
    * data-public2: repo on public Artifactory with anonymous access

    Note, that the order of the repos is important.
    audb will visit the repos in the given order
    until it finds the requested database.

    """
    current_repositories = audb.config.REPOSITORIES
    public_artifactory_host = "https://audeering.jfrog.io/artifactory"
    public_s3_host = "s3.dualstack.eu-north-1.amazonaws.com"
    audb.config.REPOSITORIES = [
        audb.Repository("audb-private", public_s3_host, "s3"),
        audb.Repository("audb-public", public_s3_host, "s3"),
    ]
    if sys.version_info < (3, 13):
        audb.config.REPOSITORIES += [
            audb.Repository("data-private", public_artifactory_host, "artifactory"),
            audb.Repository("data-public", public_artifactory_host, "artifactory"),
            audb.Repository("data-public2", public_artifactory_host, "artifactory"),
        ]

    yield repository

    audb.config.REPOSITORIES = current_repositories


@pytest.fixture(scope="module", autouse=False)
def non_existing_repository():
    r"""Non-existing repository.

    Configure the following repositories:
    * non-existing: non-exsiting repo on public Artifactory
    * audb-public: repo on public Artifactory with anonymous access

    Note, that the order of the repos is important.
    audb will visit the repos in the given order
    until it finds the requested database.

    """
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [
        audb.Repository("non-existing", PUBLIC_HOST, PUBLIC_BACKEND),
        audb.Repository("audb-public", PUBLIC_HOST, PUBLIC_BACKEND),
    ]

    yield repository

    audb.config.REPOSITORIES = current_repositories


# ===== STORAGE FORMAT =====
@pytest.fixture(scope="module", autouse=False)
def storage_format():
    """Storage format of tables."""
    yield "csv"

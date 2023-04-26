import glob
import os

import pytest

import audb
import audeer


pytest.NUM_WORKERS = 5


@pytest.fixture(scope='package', autouse=True)
def cleanup_coverage_files():
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '.coverage.*',
    )
    for file in glob.glob(path):
        os.remove(file)


# ===== CACHE =====
@pytest.fixture(scope='function', autouse=True)
def cache(tmpdir):
    r"""Temp folder as cache.

    Provide a different temporary folder
    as audb cache root
    in each test.

    """
    cache = audeer.mkdir(audeer.path(tmpdir, 'cache'))
    audb.config.CACHE_ROOT = cache
    return cache


@pytest.fixture(scope='function', autouse=True)
def shared_cache(tmpdir):
    r"""Temp folder as shared cache.

    Provide a different temporary folder
    as audb shared cache root
    in each test.

    """
    cache = audeer.mkdir(audeer.path(tmpdir, 'shared_cache'))
    audb.config.SHARED_CACHE_ROOT = cache
    return cache


@pytest.fixture(scope='package', autouse=True)
def hide_default_caches():
    r"""Hide default audb cache settings during testing.

    The cache and shared cache of audb
    are handled by ``audb.config.CACHE_ROOT``,
    ``audb.config.SHARED_CACHE_ROOT``
    and can in addition be configured
    by the environment variables
    ``AUDB_CACHE_ROOT``
    and ``AUDB_SHARED_CACHE_ROOT``.

    To ensure those will not interfer with the tests
    we hide them when executing the tests.

    """
    audb.config.CACHE_ROOT = None
    audb.config.SHARED_CACHE_ROOT = None

    env_cache = os.environ.get('AUDB_CACHE_ROOT', None)
    env_shared_cache = os.environ.get('AUDB_SHARED_CACHE_ROOT', None)
    if env_cache is not None:
        del os.environ['AUDB_CACHE_ROOT']
    if env_shared_cache is not None:
        del os.environ['AUDB_SAHRED_CACHE_ROOT']

    yield

    if env_cache is not None:
        os.environ['AUDB_CACHE_ROOT'] = env_cache
    if env_shared_cache is not None:
        os.environ['AUDB_SHARED_CACHE_ROOT'] = env_shared_cache


# ===== REPOSITORIES =====
@pytest.fixture(scope='function', autouse=False)
def repository(tmpdir_factory):
    r"""Temp folder as repository.

    Provide a different temporary folder
    as repository in each test.
    This repository will be the only one visible
    inside the test.

    """
    host = tmpdir_factory.mktemp('host')
    repository = audb.Repository(
        name='data-unittests-local',
        host=host,
        backend='file-system',
    )
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [repository]

    yield repository

    audb.config.REPOSITORIES = current_repositories


@pytest.fixture(scope='module', autouse=False)
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
    host = tmpdir_factory.mktemp('host')
    repository = audb.Repository(
        name='data-unittests-local',
        host=host,
        backend='file-system',
    )
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [repository]

    yield repository

    audb.config.REPOSITORIES = current_repositories


@pytest.fixture(scope='package', autouse=True)
def hide_default_repositories():
    r"""Hide default audb repositories during testing."""
    audb.config.REPOSITORIES = []

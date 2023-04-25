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


@pytest.fixture(scope='package', autouse=True)
def cleanup_environment_variables():
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


# === CACHE ===
#
# Provide two fixtures that create tmp folders
# holding the cache and shared cache folder.
# A fresh tmp folder is used for each test.
#
@pytest.fixture(scope='function', autouse=True)
def cache(tmpdir):
    cache = audeer.mkdir(audeer.path(tmpdir, 'cache'))
    audb.config.CACHE_ROOT = cache
    return cache


@pytest.fixture(scope='function', autouse=True)
def shared_cache(tmpdir):
    cache = audeer.mkdir(audeer.path(tmpdir, 'shared_cache'))
    audb.config.SHARED_CACHE_ROOT = cache
    return cache


# === REPOSITORIES ===
#
# Provide two fixtures that create tmp folders
# holding a repository on a file-system backend.
# One fixture provides a fresh repository
# for each test,
# the other fixture allows to reuse the same repository
# across all tests in a module.
#
@pytest.fixture(scope='package', autouse=True)
def hide_default_repositories():
    audb.config.REPOSITORIES = []


@pytest.fixture(scope='module', autouse=False)
def persistent_repository(tmpdir_factory):
    host = tmpdir_factory.mktemp('host')
    repository = audb.Repository(
        name='data-unittests-local',
        host=host,
        backend='file-system',
    )
    audb.config.REPOSITORIES = [repository]
    return repository


@pytest.fixture(scope='function', autouse=False)
def repository(tmpdir):
    repository = audb.Repository(
        name='data-unittests-local',
        host=audeer.path(tmpdir, 'host'),
        backend='file-system',
    )
    current_repositories = audb.config.REPOSITORIES
    audb.config.REPOSITORIES = [repository]

    yield repository

    audb.config.REPOSITORIES = current_repositories

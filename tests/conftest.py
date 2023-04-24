import glob
import os
import shutil

import pytest

import audb
import audeer


pytest.ROOT = audeer.path(
    os.path.dirname(os.path.realpath(__file__)),
    'tmp',
)

pytest.BACKEND = 'file-system'
pytest.FILE_SYSTEM_HOST = os.path.join(pytest.ROOT, 'repo')
pytest.ID = audeer.uid()
pytest.NUM_WORKERS = 5
pytest.REPOSITORY_NAME = 'data-unittests-local'


@pytest.fixture(scope='session', autouse=True)
def cleanup_session():
    path = os.path.join(
        pytest.ROOT,
        '..',
        '.coverage.*',
    )
    for file in glob.glob(path):
        os.remove(file)
    yield
    if os.path.exists(pytest.ROOT):
        shutil.rmtree(pytest.ROOT)


@pytest.fixture(scope='module', autouse=False)
def persistent_repository(tmp_path_factory):
    host = tmp_path_factory.mktemp('host').as_posix()
    repository = audb.Repository(
        name=pytest.REPOSITORY_NAME,
        host=host,
        backend=pytest.BACKEND,
    )
    audb.config.REPOSITORIES = [repository]
    return repository


@pytest.fixture(scope='function', autouse=False)
def repository(tmpdir):
    repository = audb.Repository(
        name=pytest.REPOSITORY_NAME,
        host=audeer.path(tmpdir, 'host'),
        backend=pytest.BACKEND,
    )
    audb.config.REPOSITORIES += [repository]
    return repository


@pytest.fixture(scope='function', autouse=True)
def shared_cache(tmp_path):
    cache = tmp_path / 'shared_cache'
    cache.mkdir()
    cache = str(cache)
    os.environ['AUDB_SHARED_CACHE_ROOT'] = cache
    return cache


@pytest.fixture(scope='function', autouse=True)
def cache(tmp_path):
    cache = tmp_path / 'cache'
    cache.mkdir()
    cache = str(cache)
    os.environ['AUDB_CACHE_ROOT'] = cache
    return cache

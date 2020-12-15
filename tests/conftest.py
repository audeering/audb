import glob
import os
import shutil

import pytest

import audeer
import audfactory


pytest.ROOT = audeer.safe_path(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'tmp',
    )
)

pytest.CACHE_ROOT = audeer.mkdir(
    os.path.join(pytest.ROOT, 'cache')
)
pytest.HOST = audeer.mkdir(
    os.path.join(pytest.ROOT, 'repo')
)
pytest.REPOSITORY_PUBLIC = 'unittests-public-local'
pytest.SHARED_CACHE_ROOT = audeer.mkdir(
    os.path.join(pytest.ROOT, 'shared')
)
pytest.GROUP_ID = f'audb2.{audeer.uid()}'


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
    for repository in (
            pytest.REPOSITORY_PUBLIC,
    ):
        url = audfactory.artifactory_path(
            audfactory.server_url(
                pytest.GROUP_ID,
                repository=repository,
            ),
        )
        if url.exists():
            url.unlink()

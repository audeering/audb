import glob
import os
import shutil

import pytest

import audata.testing
import audeer
import audfactory


pytest.ROOT = audeer.safe_path(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'tmp',
    )
)

pytest.CACHE_ROOT = os.path.join(pytest.ROOT, 'cache')
pytest.GROUP_ID = f'audb2.{audeer.uid()}'
pytest.HOST = os.path.join(pytest.ROOT, 'repo')
pytest.SHARED_CACHE_ROOT = os.path.join(pytest.ROOT, 'shared')
pytest.REPOSITORY_PUBLIC = 'unittests-public-local'


db = audata.testing.create_db(minimal=True)
db.schemes['scheme'] = audata.Scheme(
    labels=['positive', 'neutral', 'negative']
)
audata.testing.add_table(
    db,
    'emotion',
    audata.define.TableType.SEGMENTED,
    num_files=5,
    columns={'emotion': ('scheme', None)}
)
db.schemes['speaker'] = audata.Scheme(
    labels=['adam', 'eve']
)
db['files'] = audata.Table(db.files)
db['files']['speaker'] = audata.Column(scheme_id='speaker')
db['files']['speaker'].set(
    ['adam', 'adam', 'eva', 'eva'],
    files=db.files[:4],
)
pytest.DATABASE = db


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

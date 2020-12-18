import os
import shutil

import pytest

import audformat.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.GROUP_ID = pytest.GROUP_ID
audb2.config.REPOSITORY_PUBLIC = pytest.REPOSITORY_PUBLIC
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = 'test_remove'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
DB_ROOT_VERSION = {
    version: os.path.join(DB_ROOT, version) for version in
    ['1.0.0', '2.0.0']
}
DB_FILES = {
    '1.0.0': [
        os.path.join('audio', 'bundle1.wav'),
        os.path.join('audio', 'bundle2.wav'),
        os.path.join('audio', 'single.wav'),
    ],
    '2.0.0': [
        os.path.join('audio', 'new.wav'),
    ],
}
BACKEND = audb2.backend.FileSystem(pytest.HOST)


def clear_root(root: str):
    root = audeer.safe_path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture(
    scope='module',
    autouse=True,
)
def fixture_publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db['files'] = audformat.Table(audformat.filewise_index(DB_FILES['1.0.0']))

    # publish 1.0.0

    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['1.0.0'])
    db.save(DB_ROOT_VERSION['1.0.0'])
    archives = {
        db.files[0]: 'bundle',
        db.files[1]: 'bundle',
    }
    audb2.publish(
        DB_ROOT_VERSION['1.0.0'], '1.0.0', archives=archives,
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    # publish 2.0.0

    db['files'].extend_index(audformat.filewise_index(DB_FILES['2.0.0']))
    audformat.testing.create_audio_files(db, DB_ROOT_VERSION['2.0.0'])
    db.save(DB_ROOT_VERSION['2.0.0'])
    audb2.publish(
        DB_ROOT_VERSION['2.0.0'], '2.0.0',
        group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.mark.parametrize(
    'remove',
    [
        DB_FILES['1.0.0'][0],  # bundle1
        DB_FILES['1.0.0'][1],  # bundle2
        DB_FILES['1.0.0'][2],  # single
        DB_FILES['2.0.0'][0],  # new
    ]
)
def test_remove(remove):

    audb2.remove_media(
        DB_NAME, remove, group_id=pytest.GROUP_ID, backend=BACKEND,
    )

    for removed_media in [False, True]:
        for version in audb2.versions(DB_NAME, backend=BACKEND):

            if remove in DB_FILES[version]:

                db = audb2.load(
                    DB_NAME, version=version, removed_media=removed_media,
                    full_path=False, backend=BACKEND,
                )
                if removed_media:
                    assert remove in db.files
                else:
                    assert remove not in db.files
                assert remove not in audeer.list_file_names(
                    os.path.join(db.meta['audb']['root'], 'audio'),
                )

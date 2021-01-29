import os
import shutil

import pytest

import audformat.testing
import audeer

import audb2


audb2.config.CACHE_ROOT = pytest.CACHE_ROOT
audb2.config.REPOSITORIES = pytest.REPOSITORIES
audb2.config.SHARED_CACHE_ROOT = pytest.SHARED_CACHE_ROOT


DB_NAME = f'test_remove-{pytest.ID}'
DB_ROOT = os.path.join(pytest.ROOT, 'db')
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


def clear_root(root: str):
    root = audeer.safe_path(root)
    if os.path.exists(root):
        shutil.rmtree(root)


@pytest.fixture
def publish_db():

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db['files'] = audformat.Table(audformat.filewise_index(DB_FILES['1.0.0']))

    # publish 1.0.0

    audformat.testing.create_audio_files(db, DB_ROOT)
    db.save(DB_ROOT)
    archives = {
        db.files[0]: 'bundle',
        db.files[1]: 'bundle',
    }
    audb2.publish(
        DB_ROOT,
        '1.0.0',
        pytest.REPOSITORY,
        archives=archives,
        backend=pytest.BACKEND,
        host=pytest.HOST,
        verbose=False,
    )

    # publish 2.0.0

    db['files'].extend_index(
        audformat.filewise_index(DB_FILES['2.0.0']),
        inplace=True,
    )
    audformat.testing.create_audio_files(db, DB_ROOT)
    db.save(DB_ROOT)
    audb2.publish(
        DB_ROOT,
        '2.0.0',
        pytest.REPOSITORY,
        backend=pytest.BACKEND,
        host=pytest.HOST,
        verbose=False,
    )

    yield

    clear_root(DB_ROOT)
    clear_root(pytest.HOST)


@pytest.mark.parametrize(
    'format',
    [
        None,
        'wav',
        'flac',
    ]
)
def test_remove(publish_db, format):

    for remove in (
            DB_FILES['1.0.0'][0],  # bundle1
            DB_FILES['1.0.0'][1],  # bundle2
            DB_FILES['1.0.0'][2],  # single
            DB_FILES['2.0.0'][0],  # new
    ):

        audb2.remove_media(DB_NAME, remove)

        for removed_media in [False, True]:

            for version in audb2.versions(DB_NAME):

                if remove in DB_FILES[version]:

                    if format is not None:
                        name, _ = os.path.splitext(remove)
                        removed_file = f'{name}.{format}'
                    else:
                        removed_file = remove

                    db = audb2.load(
                        DB_NAME,
                        version=version,
                        format=format,
                        removed_media=removed_media,
                        full_path=False,
                        num_workers=pytest.NUM_WORKERS,
                        verbose=False,
                    )

                    if removed_media:
                        assert removed_file in db.files
                    else:
                        assert removed_file not in db.files
                    assert removed_file not in audeer.list_file_names(
                        os.path.join(db.meta['audb']['root'], 'audio'),
                    )

        # remove db from cache to ensure we always get a fresh copy
        shutil.rmtree(db.meta['audb']['root'])

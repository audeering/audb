import os

import pytest

import audeer
import audformat

import audb


@pytest.mark.parametrize(
    'files, folders',
    [
        (0, 1),
        (1, 0),
        (0, 2),
        (2, 0),
        (2, 2),
        (6, 6),
    ]
)
def test_attachments(tmpdir, repository, cache, files, folders):

    expected_attachments = {}

    # Create database
    # with number of file attachments given by `files`
    # and number of folder attachmens by `folders`
    db_name = 'db'
    db_version = '1.0.0'
    db_root = audeer.mkdir(audeer.path(tmpdir, db_name))
    db = audformat.Database(db_name)
    for n in range(files):
        file_id = f'file-{n}'
        with open(audeer.path(db_root, file_id), 'w') as file:
            file.write(f'{file_id}\n')
        db.attachments[file_id] = audformat.Attachment(file_id)
        expected_attachments[file_id] = [file_id]
    for n in range(folders):
        folder_id = f'folder-{n}'
        audeer.mkdir(audeer.path(db_root, folder_id))
        file_id = f'file-{n}'
        with open(audeer.path(db_root, folder_id, file_id), 'w') as file:
            file.write(f'{file_id}\n')
        db.attachments[folder_id] = audformat.Attachment(folder_id)
        expected_attachments[folder_id] = [os.path.join(folder_id, file_id)]
    db.save(db_root)

    # Publish database
    audb.publish(db_root, db_version, repository)

    # Load database
    db = audb.load(
        db_name,
        version=db_version,
        cache_root=cache,
        verbose=False,
    )

    assert list(db.attachments) == list(expected_attachments)
    for attachment in list(db.attachments):
        assert db.attachments[attachment].files == \
            expected_attachments[attachment]
        for file in db.attachments[attachment].files:
            assert os.path.exists(audeer.path(db.root, file))

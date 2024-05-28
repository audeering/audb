import os

import audeer
import audformat

import audb


def test_loading_multiple_attachments(tmpdir, repository):
    r"""Test loading of databases containing multiple attachments.

    As described in https://github.com/audeering/audb/issues/313,
    ``audb.load_to()`` might struggle to load all attachments.

    """
    db_name = "db"
    db_version = "1.0.0"
    db_root = audeer.mkdir(tmpdir, db_name)
    db = audformat.Database(db_name)
    # Include four different folders as attachment
    folders = [
        os.path.join("a", "a"),
        os.path.join("a", "b"),
        os.path.join("b", "a"),
        os.path.join("c", "a"),
    ]
    files = [
        os.path.join("a", "a", "file-a-a-1"),
        os.path.join("a", "a", "file-a-a-2"),
        os.path.join("a", "b", "file-a-b-1"),
        os.path.join("a", "b", "file-a-b-2"),
        os.path.join("b", "a", "file-b-a-1"),
        os.path.join("b", "a", "file-b-a-2"),
        os.path.join("c", "a", "file-c-a-1"),
        os.path.join("c", "a", "file-c-a-2"),
    ]
    for folder in folders:
        audeer.mkdir(db_root, folder)
    for file in files:
        audeer.touch(db_root, file)
    db.attachments["a-a"] = audformat.Attachment("a/a/")
    db.attachments["a-b"] = audformat.Attachment("a/b/")
    db.attachments["b-a"] = audformat.Attachment("b/a/")
    db.attachments["c-a"] = audformat.Attachment("c/a/")
    db.save(db_root)

    audb.publish(db_root, db_version, repository)

    build_dir = audeer.mkdir(tmpdir, "build")
    audb.load_to(build_dir, db_name, version=db_version, verbose=False)

    for file in files:
        assert os.path.exists(audeer.path(build_dir, file))

import os

import numpy as np
import pyarrow.parquet as parquet
import pytest

import audeer
import audformat
import audiofile

import audb


@pytest.fixture
def db(tmpdir) -> str:
    r"""Database fixture.

    Creates a minimal database,
    containing a media file
    and a table.
    The database is not yet stored to disk.

    Args:
        tmpdir: tmpdir fixture

    Returns:
        database object, and build dir path

    """
    name = "test-db"
    file = "data/file1.wav"
    table = "files"
    build_dir = audeer.path(tmpdir, "build")
    data_dir = audeer.mkdir(build_dir, os.path.dirname(os.path.normpath(file)))
    audio_file = audeer.path(data_dir, os.path.basename(os.path.normpath(file)))
    signal = np.zeros((2, 1000))
    sampling_rate = 8000
    audiofile.write(audio_file, signal, sampling_rate)
    db = audformat.Database(name)
    db.schemes["speaker"] = audformat.Scheme("str")
    index = audformat.filewise_index([file])
    db[table] = audformat.Table(index)
    db[table]["speaker"] = audformat.Column(scheme_id="speaker")
    db[table]["speaker"].set(["adam"])
    db.meta["build_dir"] = build_dir

    yield db


def expected_table_checksum(path: str) -> str:
    r"""Expected checksum of table file.

    MD5 sum for CSV table,
    ``"hash"`` metadata entry for PARQUET files.

    Args:
        path: path to table file

    """
    if audeer.file_extension(path) == "parquet":
        # See https://github.com/audeering/audformat/pull/419
        return parquet.read_schema(path).metadata[b"hash"].decode()
    else:
        return audeer.md5(path)


@pytest.mark.parametrize("storage_format", ["csv", "parquet"])
def test_publish_table_storage_format(db, repository, storage_format):
    r"""Test publishing and of tables for different storage formats.

    Tables are stored as CSV or PARQUET files.
    ``audb`` is handling them slightly different:

    * CSV tables are published as ``<table-id>.zip``,
      and the table checksum is calculated
      on the original CSV file
    * PARQUET tables are published as ``<table-id>.parquet``
      and the table checksum is read
      from the header of the PARQUET file

    Args:
        db: db fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend
        storage_format: storage format of tables

    """
    storage_formats = ["csv", "parquet"]
    other_storage_format = [sf for sf in storage_formats if sf != storage_format][0]

    build_dir = db.meta["build_dir"]
    table = list(db)[0]
    file = db.files[0]
    audio_file = audeer.path(build_dir, file)

    table_file = f"db.{table}.{storage_format}"
    other_table_file = f"db.{table}.{other_storage_format}"

    db.save(build_dir, storage_format=storage_format)

    # Check database build_dir looks as expected
    assert os.path.exists(audeer.path(build_dir, table_file))
    assert not os.path.exists(audeer.path(build_dir, other_table_file))
    assert os.path.exists(audio_file)

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)

    # Check files are published to repository
    repo = audeer.path(repository.host, repository.name)
    dependency_file = "db.parquet"
    header_file = "db.yaml"
    media_file = f"{deps.archive(file)}.zip"
    if storage_format == "csv":
        meta_file = f"{table}.zip"
    elif storage_format == "parquet":
        meta_file = f"{table}.parquet"
    expected_paths = [
        audeer.path(repo, db.name, version, dependency_file),
        audeer.path(repo, db.name, version, header_file),
        audeer.path(repo, db.name, "media", version, media_file),
        audeer.path(repo, db.name, "meta", version, meta_file),
    ]
    assert audeer.list_file_names(repo, recursive=True) == expected_paths

    # Check entries of dependency table
    assert deps.tables == [table_file]
    assert deps.media == [file]
    assert deps.checksum(table_file) == expected_table_checksum(
        audeer.path(build_dir, table_file)
    )
    if storage_format == "csv":
        assert deps.archive(table_file) == table
    elif storage_format == "parquet":
        assert deps.archive(table_file) == ""

    # Load database to cache
    db = audb.load(db.name, version=version, verbose=False, full_path=False)
    assert db.files == [file]
    assert list(db) == [table]
    assert os.path.exists(audeer.path(db.root, file))
    assert os.path.exists(audeer.path(db.root, table_file))
    assert not os.path.exists(audeer.path(db.root, other_table_file))

    # Clear build dir to force audb.load_to() to load from backend
    audeer.rmdir(build_dir)

    # Update database by adding a column
    db = audb.load_to(build_dir, db.name, version="1.0.0", verbose=False)
    db[table]["object"] = audformat.Column()
    db[table]["object"].set(["!!!"])
    db.save(build_dir, storage_format=storage_format)

    # Check database build_dir looks as expected
    assert os.path.exists(audeer.path(build_dir, table_file))
    assert not os.path.exists(audeer.path(build_dir, other_table_file))
    assert os.path.exists(audio_file)

    # Publish database update
    version = "1.1.0"
    deps = audb.publish(build_dir, version, repository, previous_version="1.0.0")

    # Check files are published to repository
    expected_paths = [
        audeer.path(repo, db.name, "1.0.0", dependency_file),
        audeer.path(repo, db.name, "1.0.0", header_file),
        audeer.path(repo, db.name, "1.1.0", dependency_file),
        audeer.path(repo, db.name, "1.1.0", header_file),
        audeer.path(repo, db.name, "media", "1.0.0", media_file),
        audeer.path(repo, db.name, "meta", "1.0.0", meta_file),
        audeer.path(repo, db.name, "meta", "1.1.0", meta_file),
    ]
    assert audeer.list_file_names(repo, recursive=True) == expected_paths

    # Check entries of dependency table
    assert deps.tables == [table_file]
    assert deps.media == [file]
    assert deps.checksum(table_file) == expected_table_checksum(
        audeer.path(build_dir, table_file)
    )

    # Load database to cache
    db = audb.load(db.name, version=version, verbose=False, full_path=False)
    assert db.files == [file]
    assert list(db) == [table]
    assert os.path.exists(audeer.path(db.root, file))
    assert os.path.exists(audeer.path(db.root, table_file))
    assert not os.path.exists(audeer.path(db.root, other_table_file))
    assert "object" in db["files"].df.columns


def test_publish_table_storage_format_both(db, repository):
    r"""Test publishing of tables stored in CSV and PARQUET.

    When publishing tables,
    stored in CSV and PARQUET files at the same time,
    the CSV file should be ignored.

    Args:
        db: db fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend

    """
    build_dir = db.meta["build_dir"]
    table = list(db)[0]
    file = db.files[0]
    audio_file = audeer.path(build_dir, file)
    table_file = f"db.{table}"

    # Save table as PARQUET and CSV at the same time
    db.save(build_dir, storage_format="parquet")
    db[table].save(
        audeer.path(build_dir, table_file),
        storage_format="csv",
        update_other_formats=False,
    )

    # Check database build_dir looks as expected
    assert os.path.exists(audeer.path(build_dir, f"{table_file}.csv"))
    assert os.path.exists(audeer.path(build_dir, f"{table_file}.parquet"))
    assert os.path.exists(audio_file)

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)

    # Check files are published to repository
    repo = audeer.path(repository.host, repository.name)
    dependency_file = "db.parquet"
    header_file = "db.yaml"
    media_file = f"{deps.archive(file)}.zip"
    meta_file = f"{table}.parquet"
    expected_paths = [
        audeer.path(repo, db.name, version, dependency_file),
        audeer.path(repo, db.name, version, header_file),
        audeer.path(repo, db.name, "media", version, media_file),
        audeer.path(repo, db.name, "meta", version, meta_file),
    ]
    assert audeer.list_file_names(repo, recursive=True) == expected_paths

    # Update only CSV table
    db = audb.load_to(build_dir, db.name, version="1.0.0", verbose=False)
    db[table]["object"] = audformat.Column()
    db[table]["object"].set(["!!!"])
    db[table].save(
        audeer.path(build_dir, table_file),
        storage_format="csv",
        update_other_formats=False,
    )
    # Remove PKL file to ensure CSV file is not newer
    os.remove(audeer.path(build_dir, f"{table_file}.pkl"))

    # Check database build_dir looks as expected
    assert os.path.exists(audeer.path(build_dir, f"{table_file}.csv"))
    assert os.path.exists(audeer.path(build_dir, f"{table_file}.parquet"))
    assert not os.path.exists(audeer.path(build_dir, f"{table_file}.pkl"))
    assert os.path.exists(audio_file)

    # Publishing updated database
    version = "1.1.0"
    deps = audb.publish(build_dir, version, repository, previous_version="1.0.0")

    # Check files are published to repository.
    # Only header and dependency table change,
    # as the PARQUET table stayed the same
    expected_paths = [
        audeer.path(repo, db.name, "1.0.0", dependency_file),
        audeer.path(repo, db.name, "1.0.0", header_file),
        audeer.path(repo, db.name, "1.1.0", dependency_file),
        audeer.path(repo, db.name, "1.1.0", header_file),
        audeer.path(repo, db.name, "media", "1.0.0", media_file),
        audeer.path(repo, db.name, "meta", "1.0.0", meta_file),
    ]
    assert audeer.list_file_names(repo, recursive=True) == expected_paths

    # Load database to cache
    db = audb.load(db.name, version=version, verbose=False, full_path=False)
    assert not os.path.exists(audeer.path(db.root, f"{table_file}.csv"))
    assert "object" not in db["files"].df.columns


def test_publish_table_storage_format_pkl(db, repository):
    r"""Test publishing of tables stored in PKL.

    When publishing tables,
    stored only in PKL format,
    they should be converted to PARQUET automatically.

    Args:
        db: db fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend

    """
    build_dir = db.meta["build_dir"]
    table = list(db)[0]
    file = db.files[0]
    audio_file = audeer.path(build_dir, file)
    table_file = f"db.{table}"

    db.save(build_dir, storage_format="pkl")

    # Check database build_dir looks as expected
    assert os.path.exists(audeer.path(build_dir, f"{table_file}.pkl"))
    assert not os.path.exists(audeer.path(build_dir, f"{table_file}.csv"))
    assert not os.path.exists(audeer.path(build_dir, f"{table_file}.parquet"))
    assert os.path.exists(audio_file)

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)
    assert f"db.{table}.parquet" in deps

    # Check files are published to repository
    repo = audeer.path(repository.host, repository.name)
    dependency_file = "db.parquet"
    header_file = "db.yaml"
    media_file = f"{deps.archive(file)}.zip"
    meta_file = f"{table}.parquet"
    expected_paths = [
        audeer.path(repo, db.name, version, dependency_file),
        audeer.path(repo, db.name, version, header_file),
        audeer.path(repo, db.name, "media", version, media_file),
        audeer.path(repo, db.name, "meta", version, meta_file),
    ]
    assert audeer.list_file_names(repo, recursive=True) == expected_paths


def test_publish_table_parquet_without_hash(db, repository):
    r"""Ensure publication of tables without hash metadata work.

    Args:
        db: db fixture
        repository: repository fixture

    """
    build_dir = db.meta["build_dir"]
    table_id = list(db)[0]
    table_file = audeer.path(build_dir, f"db.{table_id}.parquet")

    # Store parquet table without metadata entry
    db.save(build_dir, storage_format="parquet")
    table = parquet.read_table(table_file)
    metadata = table.schema.metadata.copy()
    del metadata[b"hash"]
    table = table.replace_schema_metadata(metadata)
    parquet.write_table(table, table_file, compression="snappy")

    # Check hash is not in metadata
    metadata = parquet.read_metadata(table_file)
    assert b"hash" not in metadata.metadata

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)
    assert f"db.{table_id}.parquet" in deps

from collections.abc import Sequence
import os

import numpy as np
import pyarrow.parquet as parquet
import pytest

import audeer
import audformat
import audiofile

import audb


@pytest.fixture
def build_dir(tmpdir) -> str:
    r"""Build dir fixture.

    Args:
        tmpdir: tmpdir fixture

    Returns:
        path to build dir

    """
    yield audeer.mkdir(tmpdir, "build")


@pytest.fixture
def db(build_dir) -> str:
    r"""Database fixture.

    Creates a minimal database,
    containing a media file
    and a table.
    The media file is stored in the provided ``build_dir``,
    but the database is not yet stored to disk,
    enabling to select the storage format
    of the tables inside a test function.

    Args:
        build_dir: build_dir fixture

    Returns:
        database object

    """
    name = "test-db"
    file = "data/file1.wav"
    table = "files"
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

    yield db


def assert_db_published_to_repo(
    db: audformat.Database,
    deps: audb.Dependencies,
    repository: audb.Repository,
    version: str,
    storage_format: str,
    *,
    table_changed: bool = True,
):
    r"""Assert files are published to repository.

    Args:
        db: database object,
            used to collect all media files and tables
        deps: dependency table,
            used to get name of media file archives
        repository: repository the database was published to
        version: version of database
        storage_format: table storage format on repository,
            ``"csv"`` or ``"parquet"``
        table_changed: if ``True``,
            it assumes the tables have changed
            between database versions

    """
    repo = audeer.path(repository.host, repository.name)

    dependency_file = "db.parquet"
    header_file = "db.yaml"
    files = list(db.files)
    tables = list(db)
    archives = [f"{deps.archive(file)}.zip" for file in files]
    if storage_format == "csv":
        ext = "zip"
    elif storage_format == "parquet":
        ext = "parquet"
    meta_files = [f"{table}.{ext}" for table in tables]

    def repo_path(*args):
        return audeer.path(repo, db.name, *args)

    expected_paths = [
        repo_path("1.0.0", dependency_file),
        repo_path("1.0.0", header_file),
    ]
    if version == "1.1.0":
        expected_paths.append(repo_path("1.1.0", dependency_file))
        expected_paths.append(repo_path("1.1.0", header_file))
    for archive in archives:
        expected_paths.append(repo_path("media", "1.0.0", archive))
    for meta_file in meta_files:
        expected_paths.append(repo_path("meta", "1.0.0", meta_file))
    if version == "1.1.0" and table_changed:
        for meta_file in meta_files:
            expected_paths.append(repo_path("meta", "1.1.0", meta_file))

    assert audeer.list_file_names(repo, recursive=True) == expected_paths


def assert_db_saved_to_dir(
    db: audformat.Database,
    root: str,
    storage_formats: Sequence[str],
):
    r"""Assert all database files are stored to the build dir.

    Args:
        db: database
        root: path to folder the database saved to
        storage_formats: storage formats,
            the tables have been stored to the build folder

    """
    other_storage_formats = [
        storage_format
        for storage_format in ["csv", "parquet", "pkl"]
        if storage_format not in storage_formats
    ]
    tables = list(db)
    for storage_format in storage_formats:
        for table in tables:
            table_file = f"db.{table}.{storage_format}"
            assert os.path.exists(os.path.join(root, table_file))
    for storage_format in other_storage_formats:
        for table in tables:
            table_file = f"db.{table}.{storage_format}"
            assert not os.path.exists(os.path.join(root, table_file))
    for media_file in list(db.files):
        assert os.path.exists(os.path.join(root, media_file))


def assert_dependency_table(
    db: audformat.Database,
    build_dir: str,
    deps: audb.Dependencies,
    storage_format: str,
):
    r"""Assert dependency table entries.

    Args:
        db: database
        build_dir: build dir the database was published from
        deps: dependency table
        storage_format: storage format of database tables

    """
    tables = list(db)
    assert deps.tables == [f"db.{table}.{storage_format}" for table in tables]
    assert deps.media == list(db.files)
    for table in list(db):
        table_file = f"db.{table}.{storage_format}"
        assert deps.checksum(table_file) == expected_table_checksum(
            audeer.path(build_dir, table_file)
        )
        if storage_format == "csv":
            assert deps.archive(table_file) == table
        elif storage_format == "parquet":
            assert deps.archive(table_file) == ""


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


def publish_db(build_dir, db, repository, version, storage_format):
    r"""Publish database.

    Args:
        build_dir: build dir
        db: database
        repository: repository to publish database to
        version: version of database
        storage_format: storage format of database tables

    Returns:
        dependency table of published database

    """
    db.save(build_dir, storage_format=storage_format)
    return audb.publish(build_dir, version, repository)


def update_db(db: audformat.Database) -> audformat.Database:
    r"""Update database by adding a column to all tables.

    Args:
        db: database

    Returns:
        updated database

    """
    for table in list(db):
        db[table]["object"] = audformat.Column()
        db[table]["object"].set(["!!!"])
    return db


@pytest.mark.parametrize("storage_format", ["csv", "parquet"])
class TestPublishTableStorageFormat:
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
        storage_format: storage format of tables

    """

    def test_database_save(self, build_dir, db, storage_format):
        r"""Test correct files are stored to build dir.

        Args:
            build_dir: build dir fixture
            db: database fixture
            storage_format: storage format of database tables

        """
        db.save(build_dir, storage_format=storage_format)
        assert_db_saved_to_dir(db, build_dir, [storage_format])

    def test_database_publish(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
        storage_format: str,
    ):
        r"""Tests files stored in repository and dependency table entries.

        Tables are stored differently in the repository
        for different storage formats,
        and their dependency table entry looks different as well.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture
            storage_format: database table storage format

        """
        version = "1.0.0"
        deps = publish_db(build_dir, db, repository, version, storage_format)

        assert_db_published_to_repo(db, deps, repository, version, storage_format)
        assert_dependency_table(db, build_dir, deps, storage_format)

    def test_database_load(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
        storage_format: str,
    ):
        r"""Test correct files in cache after loading database.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture
            storage_format: storage format of database tables

        """
        version = "1.0.0"
        publish_db(build_dir, db, repository, version, storage_format)

        db = audb.load(db.name, version=version, verbose=False, full_path=False)
        assert_db_saved_to_dir(db, db.root, [storage_format, "pkl"])

    @pytest.mark.parametrize("pickle_tables", [True, False])
    def test_updated_database_save(
        self, build_dir, db, repository, storage_format, pickle_tables
    ):
        r"""Test correct files are stored to build dir after database update.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture
            storage_format: storage format of database tables
            pickle_tables: if ``True``,
                ``audb.load_to()`` should store tables
                as pickle files as well

        """
        # Publish first version
        version = "1.0.0"
        publish_db(build_dir, db, repository, version, storage_format)

        # Clear build dir to force audb.load_to() to load from backend
        audeer.rmdir(build_dir)
        db = audb.load_to(
            build_dir,
            db.name,
            version=version,
            pickle_tables=pickle_tables,
            verbose=False,
        )

        # Update database
        db = update_db(db)
        db.save(build_dir, storage_format=storage_format)
        if pickle_tables:
            expected_formats = [storage_format, "pkl"]
        else:
            expected_formats = [storage_format]
        assert_db_saved_to_dir(db, db.root, expected_formats)

    def test_updated_database_publish(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
        storage_format: str,
    ):
        r"""Tests files in repository and dependency table after database update.

        Tables are stored differently in the repository
        for different storage formats,
        and their dependency table entry looks different as well.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture
            storage_format: database table storage format

        """
        # Publish first version
        previous_version = "1.0.0"
        publish_db(build_dir, db, repository, previous_version, storage_format)

        # Update database
        db = update_db(db)

        # Publish second version
        version = "1.1.0"
        deps = publish_db(build_dir, db, repository, version, storage_format)

        assert_db_published_to_repo(db, deps, repository, version, storage_format)
        assert_dependency_table(db, build_dir, deps, storage_format)

    def test_updated_database_load(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
        storage_format: str,
    ):
        r"""Test correct files in cache after loading updated database.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture
            storage_format: storage format of database tables

        """
        # Publish first version
        previous_version = "1.0.0"
        publish_db(build_dir, db, repository, previous_version, storage_format)

        # Update database
        db = update_db(db)

        # Publish second version
        version = "1.1.0"
        publish_db(build_dir, db, repository, version, storage_format)

        # Load database to cache
        db = audb.load(db.name, version=version, verbose=False, full_path=False)
        assert_db_saved_to_dir(db, db.root, [storage_format, "pkl"])
        for table in list(db):
            assert "object" in db[table].df.columns


def test_publish_table_storage_format_both(db, build_dir, repository):
    r"""Test publishing of tables stored in CSV and PARQUET.

    When publishing tables,
    stored in CSV and PARQUET files at the same time,
    the CSV file should be ignored.

    Args:
        db: db fixture
        build_dir: build_dir fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend

    """
    # Save table as PARQUET and CSV at the same time
    db.save(build_dir, storage_format="parquet")
    db.save(build_dir, storage_format="csv", update_other_formats=False)

    # Check database build_dir looks as expected
    assert_db_saved_to_dir(db, build_dir, ["csv", "parquet"])

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)

    # Check files are published to repository
    assert_db_published_to_repo(db, deps, repository, version, "parquet")

    # Update only CSV table
    db = audb.load_to(build_dir, db.name, version="1.0.0", verbose=False)
    db_new = update_db(db)
    for table in list(db):
        db_new[table].save(
            audeer.path(build_dir, f"db.{table}"),
            storage_format="csv",
            update_other_formats=False,
        )
        # Remove PKL file to ensure CSV file is not newer
        os.remove(audeer.path(build_dir, f"db.{table}.pkl"))

    # Check database build_dir looks as expected
    assert_db_saved_to_dir(db, db.root, ["csv", "parquet"])

    # Publishing database from updated build_dir
    version = "1.1.0"
    deps = audb.publish(build_dir, version, repository, previous_version="1.0.0")

    # Check files are published to repository.
    # Only header and dependency table change,
    # as the PARQUET table stayed the same
    assert_db_published_to_repo(
        db,
        deps,
        repository,
        version,
        "parquet",
        table_changed=False,
    )

    # Load database to cache
    db = audb.load(db.name, version=version, verbose=False, full_path=False)
    assert_db_saved_to_dir(db, db.root, ["parquet", "pkl"])
    for table in list(db):
        assert "object" not in db[table].df.columns


def test_publish_table_storage_format_pkl(db, build_dir, repository):
    r"""Test publishing of tables stored in PKL.

    When publishing tables,
    stored only in PKL format,
    they should be converted to PARQUET automatically.

    Args:
        db: db fixture
        build_dir: build_dir fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend

    """
    # Save database with pickle tables
    db.save(build_dir, storage_format="pkl")
    assert_db_saved_to_dir(db, db.root, ["pkl"])

    # Publish database
    version = "1.0.0"
    deps = audb.publish(build_dir, version, repository)
    for table in list(db):
        assert f"db.{table}.parquet" in deps

    # Check files are published to repository
    assert_db_published_to_repo(db, deps, repository, version, "parquet")


def test_publish_table_parquet_without_hash(db, build_dir, repository):
    r"""Ensure publication of tables without hash metadata work.

    Args:
        db: db fixture
        build_dir: build_dir fixture
        repository: repository fixture

    """
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

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


def assert_files_published_to_repo(
    db: audformat.Database,
    deps: audb.Dependencies,
    repository: audb.Repository,
    version: str,
    storage_format: str,
):
    r"""Assert files are published to repository.

    Args:
        db: database object,
            used to collect all media files and tables
        deps: dependency table,
            used to get name of media file archives
        repository: repository the database was published to
        version: version of database,
            see ``db`` fixture for possible values
        storage_format: table storage format on repository,
            ``"csv"`` or ``"parquet"``

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

    expected_paths = [
        audeer.path(repo, db.name, "1.0.0", dependency_file),
        audeer.path(repo, db.name, "1.0.0", header_file),
    ]
    if version == "1.1.0":
        expected_paths += [
            audeer.path(repo, db.name, "1.1.0", dependency_file),
            audeer.path(repo, db.name, "1.1.0", header_file),
        ]
    for archive in archives:
        expected_paths.append(audeer.path(repo, db.name, "media", "1.0.0", archive))
    for meta_file in meta_files:
        expected_paths.append(audeer.path(repo, db.name, "meta", "1.0.0", meta_file))
    if version == "1.1.0":
        for meta_file in meta_files:
            expected_paths.append(
                audeer.path(repo, db.name, "meta", "1.1.0", meta_file)
            )

    assert audeer.list_file_names(repo, recursive=True) == expected_paths


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
def TestPublishTableStorageFormat():
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
        build_dir: build_dir fixture
        repository: repository fixture,
            providing a non-persistent repository
            on a file-system backend
        storage_format: storage format of tables

    """

    def other_table_file(self, db: audformat.Database, storage_format: str) -> str:
        r"""Filename of first database table with other storage format.

        csv => parquet
        parquet => csv

        Args:
            db: database
            storage_format: selected storage format

        Returns:
            table filename with other storage format

        """
        table = list(db)[0]
        if storage_format == "csv":
            ext = "parquet"
        elif storage_format == "parquet":
            ext = "csv"
        return f"db.{table}.{ext}"

    def media_file(self, db: audformat.Database) -> str:
        r"""Filename of first media file in database.

        Args:
            db: database

        Returns:
            filename of media file

        """
        return db.files[0]

    def table_file(self, db: audformat.Database, storage_format: str) -> str:
        r"""Table file name of first table in database.

        Args:
            db: database
            storage_format: storage format of databas tables

        Returns:
            table file name

        """
        table = list(db)[0]
        return f"db.{table}.{storage_format}"

    def test_database_save(self, build_dir, db, storage_format):
        r"""Test correct files are stored to build dir.

        Args:
            build_dir: build dir fixture
            db: database fixture
            storage_format: storage format of database tables

        """
        db.save(build_dir, storage_format=storage_format)
        assert os.path.exists(
            audeer.path(build_dir, self.table_file(db, storage_format))
        )
        assert not os.path.exists(
            audeer.path(build_dir, self.other_table_file(db, storage_format))
        )
        assert os.path.exists(self.media_file(db))

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
        # Publish database
        db.save(build_dir, storage_format=storage_format)
        version = "1.0.0"
        deps = audb.publish(build_dir, version, repository)

        # Check files are published to repository
        assert_files_published_to_repo(db, deps, repository, version, storage_format)

        # Check entries of dependency table
        table_file = self.table_file(db, storage_format)
        assert deps.tables == [table_file]
        assert deps.media == [file]
        assert deps.checksum(table_file) == expected_table_checksum(
            audeer.path(build_dir, table_file)
        )
        if storage_format == "csv":
            assert deps.archive(table_file) == table
        elif storage_format == "parquet":
            assert deps.archive(table_file) == ""

    def test_database_load(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
    ):
        r"""Test correct files in cache after loading database.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture

        """
        # Publish database
        db.save(build_dir, storage_format=storage_format)
        version = "1.0.0"
        deps = audb.publish(build_dir, version, repository)

        # Load database to cache
        db = audb.load(db.name, version=version, verbose=False, full_path=False)

        assert os.path.exists(audeer.path(db.root, self.media_file(db)))
        assert os.path.exists(audeer.path(db.root, self.table_file(db, storage_format)))
        assert not os.path.exists(
            audeer.path(db.root, self.other_table_file(db, storage_format))
        )

    def test_updated_database_save(self, build_dir, db, storage_format):
        r"""Test correct files are stored to build dir after database update.

        Args:
            build_dir: build dir fixture
            db: database fixture
            storage_format: storage format of database tables

        """
        # Publish first version
        db.save(build_dir, storage_format=storage_format)
        previous_version = "1.0.0"
        deps = audb.publish(build_dir, previous_version, repository)

        # Clear build dir to force audb.load_to() to load from backend
        audeer.rmdir(build_dir)
        # Load previous version of database
        db = audb.load_to(build_dir, db.name, version=previous_version, verbose=False)

        # Update database by adding a column
        db[table]["object"] = audformat.Column()
        db[table]["object"].set(["!!!"])
        db.save(build_dir, storage_format=storage_format)

        assert os.path.exists(
            audeer.path(build_dir, self.table_file(db, storage_format))
        )
        assert not os.path.exists(
            audeer.path(build_dir, self.other_table_file(db, storage_format))
        )
        assert os.path.exists(self.media_file(db))

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
        db.save(build_dir, storage_format=storage_format)
        previous_version = "1.0.0"
        deps = audb.publish(build_dir, previous_version, repository)

        # Update database by adding a column
        db[table]["object"] = audformat.Column()
        db[table]["object"].set(["!!!"])
        db.save(build_dir, storage_format=storage_format)

        # Publish second version
        db.save(build_dir, storage_format=storage_format)
        version = "1.1.0"
        deps = audb.publish(
            build_dir,
            version,
            repository,
            previous_version=previous_version,
        )

        # Check files are published to repository
        assert_files_published_to_repo(db, deps, repository, version, storage_format)

        # Check entries of dependency table
        table_file = self.table_file(db, storage_format)
        assert deps.tables == [table_file]
        assert deps.media == [file]
        assert deps.checksum(table_file) == expected_table_checksum(
            audeer.path(build_dir, table_file)
        )
        if storage_format == "csv":
            assert deps.archive(table_file) == table
        elif storage_format == "parquet":
            assert deps.archive(table_file) == ""

    def test_updated_database_load(
        self,
        build_dir: str,
        db: audformat.Database,
        repository: audb.Repository,
    ):
        r"""Test correct files in cache after loading updated database.

        Args:
            build_dir: build dir fixture
            db: database fixture
            repository: repository fixture

        """
        # Publish first version
        db.save(build_dir, storage_format=storage_format)
        previous_version = "1.0.0"
        deps = audb.publish(build_dir, previous_version, repository)

        # Update database by adding a column
        db[table]["object"] = audformat.Column()
        db[table]["object"].set(["!!!"])
        db.save(build_dir, storage_format=storage_format)

        # Publish second version
        db.save(build_dir, storage_format=storage_format)
        version = "1.1.0"
        deps = audb.publish(
            build_dir,
            version,
            repository,
            previous_version=previous_version,
        )
        # Publish database
        db.save(build_dir, storage_format=storage_format)
        version = "1.0.0"
        deps = audb.publish(build_dir, version, repository)

        # Load database to cache
        db = audb.load(db.name, version=version, verbose=False, full_path=False)

        assert os.path.exists(audeer.path(db.root, self.media_file(db)))
        assert os.path.exists(audeer.path(db.root, self.table_file(db, storage_format)))
        assert not os.path.exists(
            audeer.path(db.root, self.other_table_file(db, storage_format))
        )
        assert "object" in db["files"].df.columns


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
    assert_files_published_to_repo(db, deps, repository, version, "parquet")

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
    assert_files_published_to_repo(db, deps, repository, version, "parquet")


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

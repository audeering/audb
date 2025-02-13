import os

import numpy as np
import pandas as pd
import pytest

import audeer
import audformat
import audiofile

import audb


def create_audio_files(root: str, index: pd.Index, *, sampling_rate=16_000):
    r"""Create audio files.

    Given an index with relative paths,
    and a root folder,
    it will create audio files
    for every entry in the index.

    The audio files have a duration of 1 second,
    and have a constant magnitude of 1.

    Args:
        root: root folder of audio files
        index: name of audio files to create in ``root``
        sampling_rate: sampling rate in Hz

    """
    for file in index:
        path = audeer.path(root, file)
        signal = np.ones((1, sampling_rate))
        audiofile.write(path, signal, sampling_rate)


class TestStreaming:
    r"""Test streaming functionality of audb.

    This test tackles ``audb.stream()``,
    and its returned class ``audb.DatabaseIterator``.

    """

    name = "db"
    """Name of test database."""

    version = "1.0.0"
    """Version of test database."""

    seed = 2
    """Seed for random operations."""

    @classmethod
    @pytest.fixture(scope="class", autouse=True, params=["parquet", "csv"])
    def setup(cls, request, tmpdir_factory, persistent_repository):
        r"""Publish a database.

        This creates a database,
        consisting of 5 audio files,
        2 misc tables (1 used as scheme labels),
        and 2 tables (1 fileswise, 1 segmented).

        The database is published to a new repository
        for each ``storage_format``,
        and all tests are run for a given ``storage_format``.

        Args:
            request: request fixture to access params
            tmpdir_factory: tmpdir_factory fixture
            persistent_repository: persistent_repository fixture

        """
        storage_format = request.param

        db_root = tmpdir_factory.mktemp("build")

        db = audformat.Database(cls.name)

        # Misc table for scheme labels
        db.schemes["year-of-birth"] = audformat.Scheme("date")
        db["speaker"] = audformat.MiscTable(
            pd.Index([0, 1, 2, 3, 4], name="speaker", dtype="Int64")
        )
        db["speaker"]["year-of-birth"] = audformat.Column(scheme_id="year-of-birth")
        db["speaker"]["year-of-birth"].set([1910, 1920, 1930, 1940, 1950])
        db.schemes["speaker"] = audformat.Scheme("int", labels="speaker")

        # Misc table
        db.schemes["full-name"] = audformat.Scheme("str")
        db["acronym"] = audformat.MiscTable(
            pd.Index(["CCC"], name="acronym", dtype="string")
        )
        db["acronym"]["full-name"] = audformat.Column(scheme_id="full-name")
        db["acronym"]["full-name"].set(["Concordance Correlation Coefficient"])
        db["acronym"]["speaker"] = audformat.Column(scheme_id="speaker")
        db["acronym"]["speaker"].set([0])

        # Filewise table
        db.schemes["quality"] = audformat.Scheme("int", labels=[1, 2, 3])
        index = audformat.filewise_index([f"file{n}.wav" for n in range(5)])
        create_audio_files(db_root, index)
        db["files"] = audformat.Table(index)
        db["files"]["quality"] = audformat.Column(scheme_id="quality")
        db["files"]["quality"].set([1, 1, 2, 2, 3])
        db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
        db["files"]["speaker"].set([0, 1, 2, 3, 4])

        # Segmented table
        index = audformat.segmented_index(
            files=["file0.wav", "file0.wav", "file1.wav", "file2.wav"],
            starts=[0, 0.5, 0.1, 0.1],
            ends=[0.4, 0.9, 0.8, 0.9],
        )
        db["segments"] = audformat.Table(index)
        db["segments"]["noise"] = audformat.Column()
        db["segments"]["noise"].set(["low", "low", "low", "high"])

        db.save(db_root, storage_format=storage_format)

        # Ensure we start with an empty repository
        # for each storage format
        audeer.rmdir(persistent_repository.host)
        audeer.mkdir(persistent_repository.host, persistent_repository.name)

        audb.publish(db_root, cls.version, persistent_repository)

        yield

        # Clean up persistent repository at end
        audeer.rmdir(persistent_repository.host)
        audeer.mkdir(persistent_repository.host, persistent_repository.name)

    @pytest.mark.parametrize("table", ["files", "segments", "speaker", "acronym"])
    @pytest.mark.parametrize("batch_size", [0, 1, 2, 10])
    def test_batch_size(self, table: str, batch_size: int):
        r"""Test table batching.

        If batch size is 0,
        no batch should be returned.
        If batch size is greater than table length,
        a single batch,
        containing the whole table,
        should be returned.

        Args:
            table: table to stream
            batch_size: number of table rows per batch

        """
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            batch_size=batch_size,
            only_metadata=True,
            full_path=False,
            verbose=False,
        )
        batches = [df for df in db]
        # Create expected dataframes from original table
        expected_df = audb.load_table(
            self.name,
            table,
            version=self.version,
            verbose=False,
        )
        if batch_size == 0:
            assert batches == []
        else:
            pd.testing.assert_frame_equal(pd.concat(batches), expected_df)
        if batch_size > len(expected_df):
            assert len(batches) == 1

    @pytest.mark.parametrize("table", ["files", "acronym"])
    @pytest.mark.parametrize("batch_size", [0, 1, 2, 10])
    @pytest.mark.parametrize("buffer_size", [0, 1, 2, 10])
    def test_buffer_size(self, table: str, batch_size: int, buffer_size: int):
        r"""Test buffer size, when shuffling table batches.

        If batch size is 0 or buffer size is 0,
        no batch should be returned.

        Args:
            table: table to stream
            batch_size: number of table rows per batch
            buffer_size: size of buffer to read table,
                before shuffling

        """
        np.random.seed(self.seed)
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            batch_size=batch_size,
            buffer_size=buffer_size,
            shuffle=True,
            only_metadata=True,
            full_path=False,
            verbose=False,
        )
        batches = [df for df in db]
        # Create expected dataframes from original table
        np.random.seed(self.seed)
        expected_df = audb.load_table(
            self.name,
            table,
            version=self.version,
            verbose=False,
        )
        if batch_size == 0 or buffer_size == 0:
            assert batches == []
        else:
            df = pd.concat(batches)
            # Ensure data is shuffled (besides a buffer size of 1)
            if buffer_size == 1 or len(df) == 1:
                assert list(df.index) == list(expected_df.index)
            else:
                assert list(df.index) != list(expected_df.index)
            # Ensure all index entries appear in shuffled batches
            assert sorted(list(df.index)) == sorted(list(expected_df.index))

    @pytest.mark.parametrize(
        "table, expected_tables, expected_schemes",
        [
            ("speaker", ["speaker"], ["year-of-birth"]),
            (
                "acronym",
                ["acronym", "speaker"],
                ["year-of-birth", "full-name", "speaker"],
            ),
            ("files", ["files", "speaker"], ["year-of-birth", "quality", "speaker"]),
        ],
    )
    def test_db_cleanup(
        self,
        table: str,
        expected_tables: list,
        expected_schemes: list,
    ):
        r"""Test removal of non-selected tables and schemes.

        The database object (``audb.DatabaseIterator``),
        should not contain unneeded tables or schemes.
        If a misc table is used as scheme labels
        in a scheme of the requested table,
        it and its schemes,
        will also be part of the database object.

        Args:
            table: table to stream
            expected_tables: expected tables in database
            expected_schemes: expected schemes in table

        """
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            only_metadata=True,
            verbose=False,
        )
        assert sorted(list(db.tables) + list(db.misc_tables)) == sorted(expected_tables)
        assert list(db.schemes) == sorted(expected_schemes)

    @pytest.mark.parametrize("full_path", [False, True])
    def test_full_path(self, full_path: bool):
        r"""Test full path in tables.

        Args:
            full_path: if ``True``,
                the path to media files
                should start with ``db.root``

        """
        db = audb.stream(
            self.name,
            "files",
            only_metadata=True,
            full_path=full_path,
            verbose=False,
        )
        df = next(db)
        path = df.index[0]
        if full_path:
            assert path == os.path.join(db.root, "file0.wav")
        else:
            assert path == "file0.wav"

    @pytest.mark.parametrize(
        "table, map",
        [
            ("acronym", {"speaker": "year-of-birth"}),
            ("acronym", {"speaker": ["year-of-birth", "speaker"]}),
            ("acronym", {"speaker": ["speaker", "year-of-birth"]}),
            ("files", {"speaker": "year-of-birth"}),
        ],
    )
    def test_map(self, table: str, map: dict):
        r"""Test mapping of scheme labels.

        Args:
            table: table to stream
            map: mapping of column with scheme labels

        """
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            map=map,
            batch_size=16,
            only_metadata=True,
            verbose=False,
        )
        df = next(db)
        expected_df = audb.load_table(
            self.name,
            table,
            version=self.version,
            map=map,
            verbose=False,
        )
        pd.testing.assert_frame_equal(df, expected_df)

    @pytest.mark.parametrize(
        "only_metadata, table, expected_number_of_media_files",
        [
            (True, "files", 5),
            (False, "files", 5),
            (True, "segments", 3),
            (False, "segments", 3),
            (True, "acronym", 0),
            (False, "acronym", 0),
            (True, "speaker", 0),
            (False, "speaker", 0),
        ],
    )
    def test_only_metadata(
        self,
        only_metadata: bool,
        table: str,
        expected_number_of_media_files: int,
    ):
        r"""Test streaming with and without media files.

        Args:
            only_metadata: if ``True``,
                only the table should be streamed
            table: table to stream
            expected_number_of_media_files: expected number of downloaded media files

        """
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            only_metadata=only_metadata,
            verbose=False,
        )
        next(db)
        assert len(db.files) == expected_number_of_media_files
        if not only_metadata:
            for file in db.files:
                assert os.path.exists(file)

    @pytest.mark.parametrize("shuffle", [False, True])
    @pytest.mark.parametrize("table", ["files", "segments", "speaker", "acronym"])
    def test_shuffle(self, shuffle: bool, table: str):
        r"""Test table batch shuffling.

        Args:
            shuffle: if returned table rows should be shuffled
            table: table to stream

        """
        np.random.seed(self.seed)
        db = audb.stream(
            self.name,
            table,
            version=self.version,
            batch_size=16,
            buffer_size=16,
            shuffle=shuffle,
            only_metadata=True,
            full_path=False,
            verbose=False,
        )
        df = next(db)
        # Create expected dataframe from original table
        np.random.seed(self.seed)
        expected_df = audb.load_table(
            self.name,
            table,
            version=self.version,
            verbose=False,
        )
        if shuffle:
            expected_df = expected_df.sample(frac=1)
        pd.testing.assert_frame_equal(df, expected_df)

    @pytest.mark.parametrize(
        "table, error, error_msg",
        [
            (
                "non-existent",
                ValueError,
                "Could not find the table 'non-existent' in db v1.0.0",
            ),
        ],
    )
    def test_errors(self, table: str, error, error_msg: str):
        with pytest.raises(error, match=error_msg):
            audb.stream(
                self.name,
                table,
                version=self.version,
                verbose=False,
            )


def test_database_iterator_error():
    db = audformat.Database("db")
    db["some"] = audformat.Table()
    table = "some"
    error_msg = "Can't instantiate abstract class DatabaseIterator"
    with pytest.raises(TypeError, match=error_msg):
        audb.DatabaseIterator(
            db,
            table,
            version=None,
            map=None,
            batch_size=16,
            shuffle=False,
            buffer_size=100_000,
            only_metadata=False,
            bit_depth=None,
            channels=None,
            format=None,
            mixdown=False,
            sampling_rate=None,
            full_path=False,
            cache_root=None,
            num_workers=1,
            verbose=False,
        )

import os
import shutil
import tempfile
import threading
import time
from unittest import mock

import pytest

import audeer
import audformat

from audb.core import define
from audb.core.dependencies import Dependencies
from audb.core.flavor import Flavor
from audb.core.load import _database_check_complete
from audb.core.load import _database_is_complete


@pytest.fixture
def temp_db_root():
    """Create a temporary directory for database root."""
    db_root = tempfile.mkdtemp()
    yield db_root
    shutil.rmtree(db_root)


@pytest.fixture
def mock_database():
    """Create a mock database for testing."""
    db = audformat.Database(name="test_db")
    db.meta["audb"] = {
        "root": "/tmp/test_db",
        "version": "1.0.0",
        "flavor": {},
        "complete": False,
    }
    return db


def test_complete_file_creation(temp_db_root, mock_database):
    """Test that .complete file is created when database is complete."""
    # Create a mock dependencies object
    deps = mock.Mock(spec=Dependencies)
    deps.attachments = []
    deps.tables = []
    deps.media = []
    deps.removed = mock.Mock(return_value=False)

    # Create a mock flavor
    flavor = Flavor()

    # Update the database root in metadata
    mock_database.meta["audb"]["root"] = temp_db_root

    # Create header file
    header_file = os.path.join(temp_db_root, define.HEADER_FILE)
    audeer.mkdir(os.path.dirname(header_file))
    mock_database.save(temp_db_root, header_only=True)

    # Call _database_check_complete
    _database_check_complete(mock_database, temp_db_root, flavor, deps)

    # Check that .complete file was created
    complete_file = os.path.join(temp_db_root, define.COMPLETE_FILE)
    assert os.path.exists(complete_file), "Complete file should be created"

    # Check that database is marked complete in metadata
    assert mock_database.meta["audb"]["complete"] is True


def test_complete_file_detection():
    """Test that _database_is_complete detects .complete file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a database with .complete file
        db = audformat.Database(name="test_db")
        db.meta["audb"] = {
            "root": temp_dir,
            "version": "1.0.0",
            "flavor": {},
            "complete": False,  # metadata says not complete
        }

        # Create .complete file
        complete_file = os.path.join(temp_dir, define.COMPLETE_FILE)
        audeer.touch(complete_file)

        # _database_is_complete should return True due to .complete file
        assert _database_is_complete(db) is True


def test_complete_file_fallback_to_metadata():
    """Test that _database_is_complete falls back to metadata when no .complete file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a database without .complete file
        db = audformat.Database(name="test_db")
        db.meta["audb"] = {
            "root": temp_dir,
            "version": "1.0.0",
            "flavor": {},
            "complete": True,  # metadata says complete
        }

        # No .complete file exists, should fall back to metadata
        assert _database_is_complete(db) is True

        # Test with incomplete metadata
        db.meta["audb"]["complete"] = False
        assert _database_is_complete(db) is False


def test_complete_file_no_metadata():
    """Test _database_is_complete when no audb metadata exists."""
    db = audformat.Database(name="test_db")
    # No audb metadata at all
    assert _database_is_complete(db) is False


def test_load_function_skips_lock_when_complete(temp_db_root):
    """Test that load function skips locking when .complete file exists."""
    # Create .complete file
    complete_file = os.path.join(temp_db_root, define.COMPLETE_FILE)
    audeer.touch(complete_file)

    # Create a minimal database header
    header_file = os.path.join(temp_db_root, define.HEADER_FILE)
    audeer.mkdir(os.path.dirname(header_file))

    db = audformat.Database(name="test_db")
    db.meta["audb"] = {
        "root": temp_db_root,
        "version": "1.0.0",
        "flavor": {},
        "complete": True,
    }
    db.save(temp_db_root, header_only=True)

    # Mock the FolderLock to ensure it's not called
    with mock.patch("audb.core.load.FolderLock") as mock_lock:
        mock_lock.return_value.__enter__ = mock.Mock(return_value=mock_lock)
        mock_lock.return_value.__exit__ = mock.Mock()

        # Mock other dependencies
        with mock.patch("audb.core.load.dependencies") as mock_deps:
            # Create a proper mock dependencies object
            mock_dep_instance = mock.Mock()
            mock_dep_instance.return_value = mock.Mock()
            mock_dep_instance.return_value.loc = mock.Mock()
            mock_dep_instance.return_value.loc.__getitem__ = mock.Mock(
                return_value=mock.Mock()
            )
            mock_dep_instance.attachments = []
            mock_dep_instance.tables = []
            mock_dep_instance.media = []
            mock_dep_instance.removed = mock.Mock(return_value=False)
            mock_deps.return_value = mock_dep_instance

            with mock.patch("audb.core.load.latest_version") as mock_version:
                mock_version.return_value = "1.0.0"

                with mock.patch(
                    "audb.core.load.database_cache_root"
                ) as mock_cache_root:
                    mock_cache_root.return_value = temp_db_root

                    with mock.patch("audb.core.load.filter_deps") as mock_filter:
                        mock_filter.return_value = []

                        with mock.patch(
                            "audb.core.load._misc_tables_used_in_scheme"
                        ) as mock_misc:
                            mock_misc.return_value = []

                            with mock.patch(
                                "audb.core.load._files_duration"
                            ) as mock_duration:
                                mock_duration.return_value = None

                                # Import and call load function
                                from audb.core.load import load

                                # This should not call FolderLock
                                result = load("test_db", version="1.0.0", verbose=False)

                                # Verify FolderLock was not called
                                mock_lock.assert_not_called()

                                # Verify result is not None (successful load)
                                assert result is not None


def test_load_function_uses_lock_when_not_complete(temp_db_root):
    """Test that load function uses locking when .complete file doesn't exist."""
    # Don't create .complete file

    # Create a minimal database header
    header_file = os.path.join(temp_db_root, define.HEADER_FILE)
    audeer.mkdir(os.path.dirname(header_file))

    db = audformat.Database(name="test_db")
    db.meta["audb"] = {
        "root": temp_db_root,
        "version": "1.0.0",
        "flavor": {},
        "complete": False,
    }
    db.save(temp_db_root, header_only=True)

    # Mock the FolderLock to track if it's called
    with mock.patch("audb.core.load.FolderLock") as mock_lock:
        mock_lock.return_value.__enter__ = mock.Mock(return_value=mock_lock)
        mock_lock.return_value.__exit__ = mock.Mock()

        # Mock other dependencies
        with mock.patch("audb.core.load.dependencies") as mock_deps:
            # Create a proper mock dependencies object
            mock_dep_instance = mock.Mock()
            mock_dep_instance.return_value = mock.Mock()
            mock_dep_instance.return_value.loc = mock.Mock()
            mock_dep_instance.return_value.loc.__getitem__ = mock.Mock(
                return_value=mock.Mock()
            )
            mock_dep_instance.attachments = []
            mock_dep_instance.tables = []
            mock_dep_instance.media = []
            mock_dep_instance.removed = mock.Mock(return_value=False)
            mock_deps.return_value = mock_dep_instance

            with mock.patch("audb.core.load.latest_version") as mock_version:
                mock_version.return_value = "1.0.0"

                with mock.patch(
                    "audb.core.load.database_cache_root"
                ) as mock_cache_root:
                    mock_cache_root.return_value = temp_db_root

                    with mock.patch("audb.core.load.filter_deps") as mock_filter:
                        mock_filter.return_value = []

                        with mock.patch(
                            "audb.core.load._misc_tables_used_in_scheme"
                        ) as mock_misc:
                            mock_misc.return_value = []

                            with mock.patch(
                                "audb.core.load._files_duration"
                            ) as mock_duration:
                                mock_duration.return_value = None

                                # Import and call load function
                                from audb.core.load import load

                                # This should use FolderLock
                                load("test_db", version="1.0.0", verbose=False)

                                # Verify FolderLock was called
                                mock_lock.assert_called_once_with(
                                    temp_db_root, timeout=define.TIMEOUT
                                )


def test_concurrent_completion_race_condition():
    """Test race condition where multiple processes try to complete database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create database files
        db = audformat.Database(name="test_db")
        db.meta["audb"] = {
            "root": temp_dir,
            "version": "1.0.0",
            "flavor": {},
            "complete": False,
        }

        db.save(temp_dir, header_only=True)

        # Mock dependencies
        deps = mock.Mock(spec=Dependencies)
        deps.attachments = []
        deps.tables = []
        deps.media = []
        deps.removed = mock.Mock(return_value=False)

        flavor = Flavor()

        # Simulate two processes trying to complete the database
        complete_file = os.path.join(temp_dir, define.COMPLETE_FILE)
        results = []

        def complete_database():
            """Simulate database completion."""
            try:
                # Create a fresh database instance for each thread
                db_instance = audformat.Database(name="test_db")
                db_instance.meta["audb"] = {
                    "root": temp_dir,
                    "version": "1.0.0",
                    "flavor": {},
                    "complete": False,
                }
                # Small delay to increase chance of race condition
                time.sleep(0.01)
                _database_check_complete(db_instance, temp_dir, flavor, deps)
                results.append(True)
            except Exception as e:
                results.append(f"Error: {e}")

        # Start two threads to simulate concurrent completion
        thread1 = threading.Thread(target=complete_database)
        thread2 = threading.Thread(target=complete_database)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        # Both should succeed or at least not fail catastrophically
        assert len(results) == 2
        # At least one should succeed
        assert any(result is True for result in results)

        # .complete file should exist
        assert os.path.exists(complete_file)


def test_complete_file_constant():
    """Test that COMPLETE_FILE constant is properly defined."""
    from audb.core import define

    assert hasattr(define, "COMPLETE_FILE")
    assert define.COMPLETE_FILE == ".complete"


def test_database_check_complete_no_complete_files():
    """Test _database_check_complete when files are missing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create database
        db = audformat.Database(name="test_db")
        db.meta["audb"] = {
            "root": temp_dir,
            "version": "1.0.0",
            "flavor": {},
            "complete": False,
        }

        # Create header file
        db.save(temp_dir, header_only=True)

        # Mock dependencies with missing files
        deps = mock.Mock(spec=Dependencies)
        deps.attachments = ["missing_attachment.txt"]
        deps.tables = []
        deps.media = []
        deps.removed = mock.Mock(return_value=False)

        flavor = Flavor()

        # Call _database_check_complete - should not mark as complete
        _database_check_complete(db, temp_dir, flavor, deps)

        # .complete file should not be created
        complete_file = os.path.join(temp_dir, define.COMPLETE_FILE)
        assert not os.path.exists(complete_file)

        # Database should not be marked complete
        assert db.meta["audb"]["complete"] is False

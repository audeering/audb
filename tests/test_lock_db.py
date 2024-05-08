import os
import sys
import threading
import time

import pytest

import audbackend
import audeer
import audformat.testing

import audb


DB_NAME = "test_lock"


class SlowFileSystem(audbackend.backend.FileSystem):
    r"""Emulate a slow file system.

    Introduces a short delay when getting a file from the backend.
    This ensures that timeouts are reached in the tests.

    """

    def _get_file(self, *args):
        time.sleep(0.1)
        super()._get_file(*args)


audb.Repository.register("slow-file-system", SlowFileSystem)


class CrashFileSystem(audbackend.backend.FileSystem):
    r"""Emulate a file system that crashes.

    Raises an exception when getting a file from the backend.

    """

    def _get_file(self, *args):
        raise RuntimeError()


audb.Repository.register("crash-file-system", CrashFileSystem)


def lock_paths(cache):
    r"""Return list of lock file locations."""
    paths = []
    for version in audb.versions(DB_NAME):
        paths.append(
            audeer.path(
                cache,
                DB_NAME,
                version,
                ".lock",
            )
        )
        paths.append(
            audeer.path(
                cache,
                DB_NAME,
                version,
                audb.Flavor().short_id,
                ".lock",
            )
        )
    return paths


@pytest.fixture(
    scope="function",
    autouse=True,
)
def assert_lock_file_is_deleted():
    r"""Tests if all lock files are deleted."""
    assert not any(
        [os.path.exists(path) for path in lock_paths(audb.default_cache_root())]
    )
    yield
    assert not any(
        [os.path.exists(path) for path in lock_paths(audb.default_cache_root())]
    )


@pytest.fixture(
    scope="function",
    autouse=True,
)
def set_repositories(persistent_repository, request):
    r"""Access module wide repository with custom backends."""
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=persistent_repository.name,
            host=persistent_repository.host,
            backend=request.param,
        ),
    ]


@pytest.fixture(
    scope="module",
    autouse=True,
)
def dbs(tmpdir_factory, persistent_repository):
    r"""Publish databases.

    This publishes a database with the name ``DB_NAME``
    and the versions 1.0.0 and 2.0.0
    to a module wide repository.

    """
    db_root = tmpdir_factory.mktemp("db")

    # create db

    db = audformat.testing.create_db(minimal=True)
    db.name = DB_NAME
    db.schemes["scheme"] = audformat.Scheme()
    audformat.testing.add_table(
        db,
        "table",
        "filewise",
        num_files=[0, 1, 2],
    )
    db.attachments["file"] = audformat.Attachment("extra/file.txt")
    db.attachments["folder"] = audformat.Attachment("extra/folder")
    audeer.mkdir(db_root, "extra/folder/sub-folder")
    for file in [
        "extra/file.txt",
        "extra/folder/file1.txt",
        "extra/folder/file2.txt",
        "extra/folder/sub-folder/file3.txt",
    ]:
        with open(audeer.path(db_root, file), "w") as fp:
            fp.write("Some text")
    db.save(db_root)
    audformat.testing.create_audio_files(db)

    # publish 1.0.0

    audb.publish(
        db_root,
        "1.0.0",
        persistent_repository,
        verbose=False,
    )

    # publish 2.0.0

    audformat.testing.add_table(
        db,
        "empty",
        "filewise",
        num_files=0,
    )
    db.save(db_root)
    audb.publish(
        db_root,
        "2.0.0",
        persistent_repository,
        previous_version="1.0.0",
        verbose=False,
    )


def load_deps():
    return audb.dependencies(
        DB_NAME,
        version="1.0.0",
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers",
    [
        10,
    ],
)
def test_lock_dependencies(
    set_repositories,
    multiprocessing,
    num_workers,
):
    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    result = audeer.run_tasks(
        load_deps,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers


def load_header():
    return audb.info.header(
        DB_NAME,
        version="1.0.0",
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers",
    [
        10,
    ],
)
def test_lock_header(set_repositories, multiprocessing, num_workers):
    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    result = audeer.run_tasks(
        load_header,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers


def load_db(timeout):
    return audb.load(
        DB_NAME,
        version="1.0.0",
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers, timeout, expected",
    [
        (2, -1, 2),
        (2, 9999, 2),
        (2, 0, 1),
    ],
)
def test_lock_load(
    set_repositories,
    multiprocessing,
    num_workers,
    timeout,
    expected,
):
    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    warns = not multiprocessing and num_workers != expected
    params = [([timeout], {})] * num_workers
    if warns:
        with pytest.warns(
            UserWarning,
            match=audb.core.define.TIMEOUT_MSG,
        ):
            result = audeer.run_tasks(
                load_db,
                params,
                num_workers=num_workers,
                multiprocessing=multiprocessing,
            )
    else:
        result = audeer.run_tasks(
            load_db,
            params,
            num_workers=num_workers,
            multiprocessing=multiprocessing,
        )
    result = [x for x in result if x is not None]

    assert len(result) == expected


@pytest.mark.parametrize(
    "set_repositories",
    ["crash-file-system"],
    indirect=True,
)
def test_lock_load_crash(set_repositories):
    with pytest.raises(audbackend.BackendError):
        load_db(-1)


@pytest.mark.parametrize(
    "set_repositories",
    ["file-system"],
    indirect=True,
)
def test_lock_load_from_cached_versions(
    persistent_repository,
    set_repositories,
):
    # ensure immediate timeout if cache folder is locked
    cached_version_timeout = audb.core.define.CACHED_VERSIONS_TIMEOUT
    audb.core.define.CACHED_VERSIONS_TIMEOUT = 0

    # load version 1.0.0
    db_v1 = audb.load(
        DB_NAME,
        version="1.0.0",
        verbose=False,
    )

    # load new files added in version 2.0.0
    audb.load(
        DB_NAME,
        version="2.0.0",
        tables="empty",
        verbose=False,
    )

    # switch to crash backend to ensure remaining files
    # must be copied from version 1.0.0
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=persistent_repository.name,
            host=persistent_repository.host,
            backend="crash-file-system",
        ),
    ]

    # lock cache folder of version 1.0.0
    def lock_v1():
        with audb.core.lock.FolderLock(db_v1.root):
            event.wait()

    event = threading.Event()
    thread = threading.Thread(target=lock_v1)
    thread.start()

    # -> loading missing table from cache fails
    with pytest.raises(audbackend.BackendError):
        audb.load(
            DB_NAME,
            version="2.0.0",
            tables="table",
            only_metadata=True,
            verbose=False,
        )

    # release cache folder of version 1.0.0
    event.set()
    thread.join()

    # -> loading missing table from cache succeeds
    audb.load(
        DB_NAME,
        version="2.0.0",
        tables="table",
        only_metadata=True,
        verbose=False,
    )

    # lock cache folder of version 1.0.0
    event.clear()
    thread = threading.Thread(target=lock_v1)
    thread.start()

    # -> loading missing media from cache fails
    with pytest.raises(audbackend.BackendError):
        audb.load(
            DB_NAME,
            version="2.0.0",
            verbose=False,
        )

    # release cache folder of version 1.0.0
    event.set()
    thread.join()

    # -> loading missing media from cache succeeds
    audb.load(
        DB_NAME,
        version="2.0.0",
        verbose=False,
    )

    # reset timeout
    audb.core.define.CACHED_VERSIONS_TIMEOUT = cached_version_timeout


def load_attachment():
    return audb.load_attachment(
        DB_NAME,
        "folder",
        version="1.0.0",
        verbose=False,
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers",
    [
        4,
    ],
)
def test_lock_load_attachment(
    set_repositories,
    multiprocessing,
    num_workers,
):
    # Avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    result = audeer.run_tasks(
        load_attachment,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers


def load_media(timeout):
    return audb.load_media(
        DB_NAME,
        "audio/001.wav",
        version="1.0.0",
        timeout=timeout,
        verbose=False,
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers, timeout, expected",
    [
        (2, -1, 2),
        (2, 9999, 2),
        (2, 0, 1),
    ],
)
def test_lock_load_media(
    set_repositories,
    multiprocessing,
    num_workers,
    timeout,
    expected,
):
    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    warns = not multiprocessing and num_workers != expected
    params = [([timeout], {})] * num_workers
    if warns:
        with pytest.warns(
            UserWarning,
            match=audb.core.define.TIMEOUT_MSG,
        ):
            result = audeer.run_tasks(
                load_media,
                params,
                num_workers=num_workers,
                multiprocessing=multiprocessing,
            )
    else:
        result = audeer.run_tasks(
            load_media,
            params,
            num_workers=num_workers,
            multiprocessing=multiprocessing,
        )
    result = [x for x in result if x is not None]

    assert len(result) == expected


def load_table():
    return audb.load_table(
        DB_NAME,
        "table",
        version="1.0.0",
        verbose=False,
    )


@pytest.mark.parametrize(
    "set_repositories",
    ["slow-file-system"],
    indirect=True,
)
@pytest.mark.parametrize(
    "multiprocessing",
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize(
    "num_workers",
    [
        10,
    ],
)
def test_lock_load_table(set_repositories, multiprocessing, num_workers):
    # avoid
    # AttributeError: module pytest has no attribute CACHE_ROOT
    # when multiprocessing=True on Windows and macOS
    if multiprocessing and sys.platform in ["win32", "darwin"]:
        return

    result = audeer.run_tasks(
        load_table,
        [([], {})] * num_workers,
        num_workers=num_workers,
        multiprocessing=multiprocessing,
    )

    assert len(result) == num_workers

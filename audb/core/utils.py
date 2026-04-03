from collections.abc import Sequence
from contextlib import contextmanager
import os
import sys
import threading
import warnings

import pyarrow.parquet as parquet

import audbackend
import audeer
import audeer.core.tqdm
from audeer.core.tqdm import _ANSI_COLOR_RESET
from audeer.core.tqdm import _ansi_colour
import audeer.core.utils

from audb.core import define
from audb.core.config import config
from audb.core.repository import Repository


def _status_frames():
    """Build animated status frames from audeer progress bar config.

    Returns a list of strings representing animation frames.
    A highlighted symbol bounces left-to-right-to-left
    across 3 positions, e.g. ``["╸╸╸", "╸╸╸", "╸╸╸"]``
    with ANSI colours applied.

    """
    bar = audeer.config.TQDM_BAR
    fg = audeer.config.TQDM_COLOUR
    bg = audeer.config.TQDM_BG_COLOUR

    if bar is None:
        bar = "."
    char = bar[0]

    if fg and bg:
        fg_code = _ansi_colour(fg)
        bg_code = _ansi_colour(bg)
        bright = f"{fg_code}{char}{_ANSI_COLOR_RESET}"
        dim = f"{bg_code}{char}{_ANSI_COLOR_RESET}"
    else:
        bright = char
        dim = char

    n = 3
    # Bounce pattern: 0, 1, 2, 1
    indices = list(range(n)) + list(range(n - 2, 0, -1))
    frames = []
    for active in indices:
        parts = [bright if i == active else dim for i in range(n)]
        frames.append("".join(parts))
    return frames


@contextmanager
def status_line(verbose=True):
    r"""Show an animated status indicator between progress bars.

    Displays a bouncing symbol animation on stderr
    using the progress bar character and colours
    from ``audeer.config``.
    When a progress bar starts, the animation is paused.
    When the progress bar finishes, it resumes.

    This works by temporarily wrapping ``audeer.progress_bar``
    so that each bar pauses the animation on open
    and resumes it on close.

    When ``verbose`` is ``False``, no output is produced
    and ``audeer.progress_bar`` is not wrapped.

    """
    if not verbose:
        yield
        return

    frames = _status_frames()
    frame_idx = [0]
    timer = [None]
    lock = threading.Lock()
    active = [True]

    def _tick():
        """Timer callback: write one frame and schedule the next."""
        with lock:
            if not active[0]:
                return  # pragma: no cover
            sys.stderr.write(f"\r{frames[frame_idx[0]]}")
            sys.stderr.flush()
            frame_idx[0] = (frame_idx[0] + 1) % len(frames)
            timer[0] = threading.Timer(0.3, _tick)
            timer[0].daemon = True
            timer[0].start()

    def _pause():
        """Stop animation and clear the line.

        Holds the lock so no timer callback can write
        between cancellation and the clear.
        """
        with lock:
            active[0] = False
            if timer[0] is not None:
                timer[0].cancel()
                timer[0] = None
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()

    def _resume():
        """Restart the animation.

        Cancels any existing timer first to prevent
        parallel chains from accumulating.
        """
        with lock:
            active[0] = True
            if timer[0] is not None:  # pragma: no cover
                timer[0].cancel()
            # Write first frame immediately under the lock
            # so nothing can sneak in before it appears
            sys.stderr.write(f"\r{frames[frame_idx[0]]}")
            sys.stderr.flush()
            frame_idx[0] = (frame_idx[0] + 1) % len(frames)
            timer[0] = threading.Timer(0.3, _tick)
            timer[0].daemon = True
            timer[0].start()

    original_progress_bar = audeer.progress_bar

    def _wrapped_progress_bar(*args, **kwargs):
        _pause()
        bar = original_progress_bar(*args, **kwargs)
        if bar.disable:
            _resume()
            return bar
        original_close = bar.close
        closed = [False]

        def _patched_close():
            if closed[0]:
                return
            closed[0] = True
            original_close()
            if active[0] is not None:
                _resume()

        bar.close = _patched_close
        return bar

    def _patch():
        """Replace progress_bar in all modules that import it."""
        audeer.progress_bar = _wrapped_progress_bar
        audeer.core.tqdm.progress_bar = _wrapped_progress_bar
        audeer.core.utils.audeer_progress_bar = _wrapped_progress_bar

    def _restore():
        """Restore original progress_bar references."""
        audeer.progress_bar = original_progress_bar
        audeer.core.tqdm.progress_bar = original_progress_bar
        audeer.core.utils.audeer_progress_bar = original_progress_bar

    _resume()
    _patch()
    try:
        yield
    finally:
        _restore()
        _pause()
        # Prevent _patched_close from restarting animation
        # if tqdm.__del__ fires during interpreter shutdown
        active[0] = None


def is_empty(path: str) -> bool:
    """Check if path is an empty folder.

    Args:
        path: path to folder

    Returns:
        ``True`` if folder is empty

    """
    with os.scandir(path) as entries:
        return next(entries, None) is None


def lookup_backend(
    name: str,
    version: str,
) -> type[audbackend.interface.Base]:
    r"""Return backend of requested database.

    If the database is stored in several repositories,
    only the first one is considered.
    The order of the repositories to look for the database
    is given by :attr:`config.REPOSITORIES`.

    Args:
        name: database name
        version: version string

    Returns:
        backend interface

    Raises:
        RuntimeError: if database is not found

    """
    return _lookup(name, version)[1]


def md5(file: str) -> str:
    r"""MD5 checksum of file.

    PARQUET files are stored non-deterministically.
    To ensure tracking changes to those files correctly,
    the checksum can be provided
    under the key ``b"hash"`` in its metadata,
    e.g. which is done when creating a PARQUET file
    with :meth:`audformat.Table.save`.

    If the key is not present in its metadata,
    or the file is not a PARQUET file
    :func:`audeer.md5` is used to calculate the checksum.

    Args:
        file: file path with extension

    Returns:
        MD5 checksum of file

    """
    ext = audeer.file_extension(file)
    if ext == "parquet":
        metadata = parquet.read_schema(file).metadata
        if b"hash" in metadata:
            return metadata[b"hash"].decode()
    return audeer.md5(file)


def mkdir_tree(
    files: Sequence[str],
    root: str,
):
    r"""Helper function to create folder tree."""
    folders = set()
    for file in files:
        folders.add(os.path.dirname(file))
    for folder in folders:
        audeer.mkdir(root, folder)


def _lookup(
    name: str,
    version: str,
) -> tuple[Repository, type[audbackend.interface.Base]]:
    r"""Helper function to look up database in all repositories.

    Returns repository, version and backend object.

    """
    for repository in config.REPOSITORIES:
        try:
            backend_interface = repository.create_backend_interface()
            backend_interface.backend.open()
        except (audbackend.BackendError, ValueError):
            continue

        header = backend_interface.join("/", name, "db.yaml")
        if backend_interface.exists(header, version, suppress_backend_errors=True):
            return repository, backend_interface
        else:
            backend_interface.backend.close()

    raise RuntimeError(f"Cannot find version '{version}' for database '{name}'.")


def timeout_warning():
    warnings.warn(
        define.TIMEOUT_MSG,
        category=UserWarning,
    )

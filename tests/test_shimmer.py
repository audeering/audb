import io
import sys

from audb.core.shimmer import BOLD
from audb.core.shimmer import RESET
from audb.core.shimmer import Shimmer


def test_stdout_write_hook():
    """Ensure stdout write hook tracks newlines and delegates."""
    buf = io.StringIO()
    shimmer = Shimmer("", "test")
    shimmer._original_stdout_write = buf.write

    result = shimmer._stdout_write_hook("hello\nworld\n")
    assert result == 12
    assert buf.getvalue() == "hello\nworld\n"
    assert shimmer._lines_below == 2


def test_stderr_write_hook():
    r"""Ensure stderr write hook detects progress-bar activity.

    A ``\r`` followed by visible content pauses the shimmer,
    and ``\r`` followed by only whitespace resumes it.

    """
    buf = io.StringIO()
    shimmer = Shimmer("", "test")
    shimmer._original_stderr_write = buf.write

    # Visible bar content after \r → pause
    shimmer._stderr_write_hook("\r50%|####")
    assert shimmer._paused is True

    # Only whitespace after \r → resume
    shimmer._stderr_write_hook("\r   \r")
    assert shimmer._paused is False


def test_write_hooks_preserve_stream_identity():
    """Ensure monkey-patching does not replace the stream objects.

    After start(), sys.stdout must still be the same object
    (not a proxy), so that attribute access like fileno(),
    isatty(), encoding, etc. continues to work.

    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    shimmer = Shimmer("", "test")
    shimmer.start()
    try:
        assert sys.stdout is original_stdout
        assert sys.stderr is original_stderr
    finally:
        shimmer.stop()
    assert sys.stdout.write is original_stdout.write
    assert sys.stderr.write is original_stderr.write


def test_render_frame():
    """Test that _render_frame highlights characters near center."""
    shimmer = Shimmer("", "abcde", width=4)

    # Center on character 2 ('c'): nearby chars should be bold
    frame = shimmer._render_frame(2.0)
    assert f"{BOLD}c{RESET}" in frame

    # Characters far from center should be plain
    assert frame.startswith("a") or frame.startswith(f"{BOLD}a{RESET}")

    # Character at fading edge (within window but low brightness)
    # should appear plain. With width=4, half=2.0, a character at
    # dist ~1.9 from center has brightness cos(1.9/2.0 * pi/2) ≈ 0.08.
    frame_edge = shimmer._render_frame(0.1)
    # 'a' is at index 0, dist=0.1 → bold; 'c' at index 2, dist=1.9 → plain
    assert f"{BOLD}a{RESET}" in frame_edge
    assert f"{BOLD}c{RESET}" not in frame_edge

    # Center far outside text: no characters should be bold
    frame_outside = shimmer._render_frame(-10.0)
    assert BOLD not in frame_outside
    assert frame_outside == "abcde"

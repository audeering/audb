import io
import sys
import time

from audb.core import shimmer as shimmer_module
from audb.core.shimmer import BOLD
from audb.core.shimmer import RESET
from audb.core.shimmer import Shimmer


def test_stdout_write_hook():
    """Ensure stdout write hook tracks newlines and delegates."""
    buf = io.StringIO()
    shimmer = Shimmer("", "test")
    shimmer._original_stdout_write = buf.write

    num_chars = shimmer._stdout_write_hook("hello\nworld\n")
    assert num_chars == 12
    assert buf.getvalue() == "hello\nworld\n"
    assert shimmer._lines_below == 2


def test_stderr_write_hook():
    r"""Ensure stderr write hook detects progress-bar activity.

    A ``\r`` followed by visible content pauses the shimmer,
    and ``\r`` followed by only whitespace resumes it.
    A ``\n`` on stderr (e.g. tqdm finishing with ``leave=True``,
    log records, warnings) scrolls the terminal and must
    advance ``_lines_below``.

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

    # tqdm leave=True finisher: trailing \n scrolls the terminal
    assert shimmer._lines_below == 0
    shimmer._stderr_write_hook("\n")
    assert shimmer._lines_below == 1
    assert shimmer._paused is False

    # Plain stderr log line with an embedded newline also scrolls
    shimmer._stderr_write_hook("oops\nmore\n")
    assert shimmer._lines_below == 3


def test_write_hooks_preserve_stream_identity(monkeypatch):
    """Ensure monkey-patching does not replace the stream objects.

    After start(), sys.stdout must still be the same object
    (not a proxy), so that attribute access like fileno(),
    isatty(), encoding, etc. continues to work.

    """
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    shimmer = Shimmer("", "test")
    shimmer.start()
    try:
        assert sys.stdout is original_stdout
        assert sys.stderr is original_stderr
        # write should be replaced with the hook
        assert sys.stdout.write == shimmer._stdout_write_hook
    finally:
        shimmer.stop()
    # After stop, write should no longer be the hook
    assert sys.stdout.write != shimmer._stdout_write_hook


def test_start_stop_tracks_newlines(monkeypatch):
    """Integration test: start installs hook, stdout tracks newlines, stop restores."""
    # Ensure stdout looks like a TTY for the shimmer to activate
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    original_write = sys.stdout.write

    shimmer = Shimmer("", "test")
    shimmer.start()
    try:
        # The hook should be installed
        assert sys.stdout.write is not original_write
        # Write text with newlines via the hooked stdout
        sys.stdout.write("line1\nline2\nline3\n")
        assert shimmer._lines_below == 3
        # Write more
        sys.stdout.write("another\n")
        assert shimmer._lines_below == 4
    finally:
        shimmer.stop()

    # After stop(), original write must be restored
    assert sys.stdout.write is original_write


def test_noop_when_not_a_tty(monkeypatch):
    """Shimmer becomes a no-op when stdout is not a TTY."""
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    original_write = sys.stdout.write

    shimmer = Shimmer("", "test")
    shimmer.start()
    # Should not have patched stdout
    assert sys.stdout.write is original_write
    assert shimmer._noop is True
    shimmer.stop()


def test_second_shimmer_becomes_noop(monkeypatch):
    """Only one Shimmer can be active; a second one becomes a no-op."""
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)

    s1 = Shimmer("", "first")
    s1.start()
    try:
        s2 = Shimmer("", "second")
        s2.start()
        assert s2._noop is True
        s2.stop()
        # s1 should still be the active shimmer
        assert shimmer_module._active_shimmer is s1
    finally:
        s1.stop()

    # After both stopped, no active shimmer
    assert shimmer_module._active_shimmer is None


def test_animate_paused_clears_bold(monkeypatch):
    """When paused, the animation writes plain text to clear bold."""
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    frames = []
    shimmer = Shimmer("", "hello", interval=0.01)

    # Patch _write_frame before start() to avoid race with animation thread
    original_write_frame = shimmer._write_frame

    def capture_frame(rendered_text):
        frames.append(rendered_text)
        original_write_frame(rendered_text)

    shimmer._write_frame = capture_frame
    shimmer.start()
    try:
        # Let animation run a couple of frames unpaused
        time.sleep(0.2)
        # At least one animated frame should contain BOLD
        assert any(BOLD in f for f in frames)
        # Simulate a progress bar pausing the shimmer
        frames.clear()
        with shimmer._lock:
            shimmer._paused = True
        # Let the animation loop pick up the paused state
        time.sleep(0.2)
        # The paused frame should be plain text without BOLD
        assert len(frames) == 1
        assert BOLD not in frames[0]
        assert frames[0] == "hello"
    finally:
        shimmer.stop()


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

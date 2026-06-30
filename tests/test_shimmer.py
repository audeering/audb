import io
import os
import sys

import numpy as np

import audeer
import audformat
import audiofile

import audb
from audb.core import shimmer as shimmer_module
from audb.core.shimmer import BOLD
from audb.core.shimmer import NORMAL
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
    r"""Ensure stderr write hook tracks newlines.

    A ``\n`` on stderr (e.g. tqdm finishing with ``leave=True``,
    log records, warnings) scrolls the terminal and must
    advance ``_lines_below``. Carriage returns from in-place
    progress-bar updates do not scroll and must be ignored.

    """
    buf = io.StringIO()
    shimmer = Shimmer("", "test")
    shimmer._original_stderr_write = buf.write

    # Carriage-return bar updates do not scroll the terminal
    assert shimmer._lines_below == 0
    shimmer._stderr_write_hook("\r50%|####")
    shimmer._stderr_write_hook("\r   \r")
    assert shimmer._lines_below == 0

    # tqdm leave=True finisher: trailing \n scrolls the terminal
    shimmer._stderr_write_hook("\n")
    assert shimmer._lines_below == 1

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
    # stdout normally exposes ``write`` as a class method, so there is
    # no instance-level override before we patch.
    assert "write" not in vars(sys.stdout)
    original_write = sys.stdout.write

    shimmer = Shimmer("", "test")
    shimmer.start()
    try:
        # The hook should be installed as an instance-level override
        assert sys.stdout.write is not original_write
        assert "write" in vars(sys.stdout)
        # Write text with newlines via the hooked stdout
        sys.stdout.write("line1\nline2\nline3\n")
        assert shimmer._lines_below == 3
        # Write more
        sys.stdout.write("another\n")
        assert shimmer._lines_below == 4
    finally:
        shimmer.stop()

    # After stop(), the instance-level override must be removed entirely
    # (not merely re-pointed at the original), so attribute lookup falls
    # back to the class method. Bound methods compare equal by value, so
    # use ``==`` rather than ``is``.
    assert "write" not in vars(sys.stdout)
    assert sys.stdout.write == original_write


def test_noop_when_not_a_tty(monkeypatch):
    """Shimmer becomes a no-op when stdout is not a TTY."""
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    original_write = sys.stdout.write

    shimmer = Shimmer("", "test")
    shimmer.start()
    # Should not have patched stdout: no instance-level override installed.
    # (Bound methods compare equal by value, not identity, so use ``==``.)
    assert "write" not in vars(sys.stdout)
    assert sys.stdout.write == original_write
    assert shimmer._noop is True
    shimmer.stop()


def test_restores_pre_existing_instance_write(monkeypatch):
    """A pre-existing instance-level ``write`` is restored, not deleted.

    When the stream already carries its own ``write`` in ``__dict__``
    (e.g. it has been wrapped by another tool), stop() must put that
    exact object back rather than removing the attribute and exposing
    a class method that does not exist on such a stream.

    """

    class _FakeTTY:
        def __init__(self):
            self.data = []
            # Pre-existing instance-level write override.
            self.write = self._write

        def _write(self, s):
            self.data.append(s)
            return len(s)

        def flush(self):
            pass

        def isatty(self):
            return True

    fake_out = _FakeTTY()
    fake_err = _FakeTTY()
    monkeypatch.setattr(sys, "stdout", fake_out)
    monkeypatch.setattr(sys, "stderr", fake_err)

    original_out_write = fake_out.write
    original_err_write = fake_err.write
    assert "write" in vars(fake_out)
    assert "write" in vars(fake_err)

    shimmer = Shimmer("", "test")
    shimmer.start()
    try:
        # Hooks installed over the pre-existing overrides.
        assert fake_out.write == shimmer._stdout_write_hook
        assert fake_err.write == shimmer._stderr_write_hook
    finally:
        shimmer.stop()

    # The original instance-level overrides are restored exactly,
    # not removed (which would have re-exposed a non-existent class method).
    assert fake_out.write is original_out_write
    assert fake_err.write is original_err_write


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


def test_render_frame():
    """Test that _render_frame highlights characters near center."""
    shimmer = Shimmer("", "abcde", width=4)

    # Center on character 2 ('c'): nearby chars should be bold.
    # A bold character is closed with NORMAL (not RESET) so following
    # base characters stay non-bold on bold-default terminals.
    frame = shimmer._render_frame(2.0)
    assert f"{BOLD}c{NORMAL}" in frame

    # The whole frame is wrapped: it starts by forcing normal intensity
    # and ends by resetting, so the base text is explicitly non-bold.
    assert frame.startswith(NORMAL)
    assert frame.endswith(RESET)

    # Character at fading edge (within window but low brightness)
    # should appear plain. With width=4, half=2.0, a character at
    # dist ~1.9 from center has brightness cos(1.9/2.0 * pi/2) ≈ 0.08.
    frame_edge = shimmer._render_frame(0.1)
    # 'a' is at index 0, dist=0.1 → bold; 'c' at index 2, dist=1.9 → plain
    assert f"{BOLD}a{NORMAL}" in frame_edge
    assert f"{BOLD}c{NORMAL}" not in frame_edge

    # Center far outside text: no characters should be bold, but the
    # text is still wrapped in normal-intensity / reset codes.
    frame_outside = shimmer._render_frame(-10.0)
    assert BOLD not in frame_outside
    assert frame_outside == f"{NORMAL}abcde{RESET}"


def test_render_frame_non_bold_on_bold_default_terminal():
    """Base text is forced non-bold, so the shimmer shows on bold terminals.

    On a terminal whose default font weight is bold, characters emitted
    bare (or a bold run closed with a full ``RESET``) render bold, which
    hides the shimmer. The frame must therefore:

    * begin with ``NORMAL`` to force the base text to non-bold;
    * close every bold character with ``NORMAL`` (not ``RESET``), so the
      base characters following the bright window do not fall back to the
      terminal's bold default;
    * use ``RESET`` exactly once, at the very end.

    """
    shimmer = Shimmer("", "abcde", width=4)
    frame = shimmer._render_frame(2.0)

    # The base text is explicitly set to normal intensity up front.
    assert frame.startswith(NORMAL)

    # RESET only ever appears once, as the trailing wrapper. If it were
    # used to close bold characters, the base text after the shimmer
    # window would inherit the terminal's bold default again.
    assert frame.count(RESET) == 1
    assert frame.endswith(RESET)

    # Every bold opener is paired with a NORMAL closer, plus the single
    # leading NORMAL that opens the frame.
    assert frame.count(NORMAL) == frame.count(BOLD) + 1
    for piece in frame.split(BOLD)[1:]:
        # The character right after BOLD is closed by NORMAL.
        assert NORMAL in piece


def test_write_frame_skips_when_scrolled_off(monkeypatch):
    """Frame is skipped once the shimmer line has scrolled off the viewport.

    Cursor-up is clamped to row 0 by the terminal and cannot reach
    into scrollback, so writing anyway would corrupt an unrelated
    visible row. Simulated here by pinning terminal height via the
    ``LINES`` env var (which ``shutil.get_terminal_size`` honours).

    """
    buf = io.StringIO()
    shimmer = Shimmer("", "test")
    shimmer._original_stdout_write = buf.write

    # Pin a small terminal height. COLUMNS must also be set for
    # get_terminal_size to take the env-var path on some platforms.
    monkeypatch.setenv("LINES", "10")
    monkeypatch.setenv("COLUMNS", "80")
    # get_terminal_size prefers an actual TTY over env vars when
    # stdout is a terminal; force the env path by detaching fileno.
    monkeypatch.setattr(
        shimmer_module.shutil,
        "get_terminal_size",
        lambda fallback=(80, 24): os.terminal_size((80, 10)),
    )

    # Still within viewport: 0 lines below + 1 = 1 < 10
    shimmer._lines_below = 0
    shimmer._write_frame("test")
    assert buf.getvalue() != ""
    buf.truncate(0)
    buf.seek(0)

    # Still within viewport: 8 lines below + 1 = 9 < 10
    shimmer._lines_below = 8
    shimmer._write_frame("test")
    assert buf.getvalue() != ""
    buf.truncate(0)
    buf.seek(0)

    # Right at the boundary: up = 10 == height → skip
    shimmer._lines_below = 9
    shimmer._write_frame("test")
    assert buf.getvalue() == ""

    # Well past the boundary: still skip, no corruption
    shimmer._lines_below = 1000
    shimmer._write_frame("test")
    assert buf.getvalue() == ""


def test_shimmer_load_table_smoke(tmp_path, monkeypatch, repository):
    """End-to-end smoke test for the Shimmer <-> load_table integration.

    The call sites in ``load.py`` / ``load_to.py`` / ``publish.py``
    that instantiate ``Shimmer`` are all marked ``# pragma: no cover``
    because ``Shimmer.start()`` becomes a no-op under pytest (stdout
    is not a TTY). This test forces ``isatty=True`` so the Shimmer
    genuinely starts its animation thread and installs the stdout /
    stderr write hooks, then runs ``audb.load_table`` against a tiny
    freshly-published fixture. It verifies:

    * the call returns correct data;
    * the Shimmer is torn down (no global reference leak);
    * the monkey-patched write methods are restored after the call.

    This is a smoke test: it does not assert anything about the
    animation frames themselves — those are covered by the unit
    tests above. Its job is to catch regressions where the hooks,
    locking, or lifecycle break the real load pipeline.

    """
    # Build a minimal database: one silent wav, one filewise table.
    build_dir = audeer.mkdir(tmp_path, "build")
    media_rel = "data/file1.wav"
    audeer.mkdir(build_dir, os.path.dirname(media_rel))
    audio_path = audeer.path(build_dir, media_rel)
    audiofile.write(audio_path, np.zeros((1, 800)), 8000)

    db_name = "shimmer-smoke-db"
    db = audformat.Database(db_name)
    db.schemes["speaker"] = audformat.Scheme("str")
    db["files"] = audformat.Table(audformat.filewise_index([media_rel]))
    db["files"]["speaker"] = audformat.Column(scheme_id="speaker")
    db["files"]["speaker"].set(["adam"])
    db.save(build_dir)

    # Publish with verbose=False so no Shimmer is created during publish,
    # keeping the shimmer activation isolated to the load_table call below.
    audb.publish(build_dir, "1.0.0", repository, verbose=False)

    # Force stdout/stderr to look like a TTY so Shimmer.start() takes the
    # real path instead of the no-op branch.
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)

    # Track that at least one Shimmer actually took the non-noop path —
    # otherwise the test would silently pass even if the TTY forcing
    # above stopped working (e.g. pytest capture changes).
    real_starts: list[bool] = []
    original_start = Shimmer.start

    def tracking_start(self):
        original_start(self)
        real_starts.append(not self._noop)

    monkeypatch.setattr(Shimmer, "start", tracking_start)

    # Sanity: no shimmer leaking in from an earlier test.
    assert shimmer_module._active_shimmer is None
    original_stdout_write = sys.stdout.write
    original_stderr_write = sys.stderr.write

    df = audb.load_table(db_name, "files", version="1.0.0", verbose=True)

    # At least one Shimmer must have been started AND taken the real path.
    assert real_starts, "no Shimmer was started — load_table path changed?"
    assert any(real_starts), "every Shimmer became a no-op — TTY guard fired"

    assert list(df.columns) == ["speaker"]
    assert len(df) == 1
    assert df["speaker"].iloc[0] == "adam"
    # The shimmer must have cleaned up after itself.
    assert shimmer_module._active_shimmer is None
    # The hooks must have been uninstalled. Bound methods compare by
    # value, not identity — a fresh attribute access returns a new
    # bound method object, so use ``==`` rather than ``is``.
    assert sys.stdout.write == original_stdout_write
    assert sys.stderr.write == original_stderr_write

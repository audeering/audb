"""Shimmer text animation for terminal output."""

from __future__ import annotations

import io
import math
import sys
import threading


# ANSI escape codes
BOLD = "\033[1m"
RESET = "\033[0m"
SAVE_CURSOR = "\033[s"
RESTORE_CURSOR = "\033[u"
CLEAR_LINE_RIGHT = "\033[K"


class _StreamProxy(io.TextIOBase):
    """Proxy for a text stream that observes writes.

    Calls *on_write* for every ``write()`` so the shimmer
    can count newlines and detect progress-bar activity.

    """

    def __init__(self, target: io.TextIOBase, on_write):
        self._target = target
        self._on_write = on_write

    def write(self, s: str) -> int:
        self._on_write(s)
        return self._target.write(s)

    def flush(self):
        self._target.flush()

    def fileno(self):
        return self._target.fileno()

    @property
    def encoding(self):
        return self._target.encoding

    def isatty(self):
        return self._target.isatty()

    def writable(self):
        return True


class Shimmer:
    """Animate text with a bright shimmer sweeping across.

    The shimmer highlights a moving window of characters
    in bold/bright while the rest stays in the default color.
    The animation runs in a background thread and uses
    ANSI cursor movement to update the animated line
    even as new content appears below it.

    Args:
        prefix: static text before the animated portion
            (e.g. ``"Get:   "``).
        text: the text to animate (e.g. the database name).
        suffix: static text after the animated portion
            (e.g. ``" v1.0.0"``).
        interval: seconds between animation frames.
        width: number of characters in the bright window.

    """

    def __init__(
        self,
        prefix: str,
        text: str,
        suffix: str = "",
        *,
        interval: float = 0.05,
        width: int = 5,
    ):
        self._prefix = prefix
        self._text = text
        self._suffix = suffix
        self._interval = interval
        self._width = width
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lines_below = 0
        self._paused = False
        self._lock = threading.Lock()
        self._original_stdout: io.TextIOBase | None = None
        self._original_stderr: io.TextIOBase | None = None
        self._stdout_proxy: _StreamProxy | None = None
        self._stderr_proxy: _StreamProxy | None = None

    def start(self):
        """Start the shimmer animation in a background thread."""
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        # Print the initial static line with a newline
        # so subsequent output appears below it.
        self._original_stdout.write(f"{self._prefix}{self._text}{self._suffix}\n")
        self._original_stdout.flush()
        self._stdout_proxy = _StreamProxy(self._original_stdout, self._on_stdout_write)
        self._stderr_proxy = _StreamProxy(self._original_stderr, self._on_stderr_write)
        sys.stdout = self._stdout_proxy
        sys.stderr = self._stderr_proxy
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the animation and restore streams."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        # Restore original streams
        if self._original_stdout is not None:
            sys.stdout = self._original_stdout
        if self._original_stderr is not None:
            sys.stderr = self._original_stderr
        # Write final static line over the animated one
        self._write_frame(self._text)

    def _on_stdout_write(self, s: str):
        """Track newlines from stdout to update line count."""
        count = s.count("\n")
        if count:
            with self._lock:
                self._lines_below += count

    def _on_stderr_write(self, s: str):
        r"""Detect progress-bar activity on stderr.

        Progress bars (tqdm) write ``\r`` followed by bar content
        to update in-place. When the bar finishes with
        ``leave=False``, tqdm clears it by writing
        ``\r`` + whitespace + ``\r``.

        We pause when we see ``\r`` followed by visible
        (non-whitespace) content, and resume when we see
        ``\n`` or the clear pattern (only whitespace after ``\r``).

        """
        with self._lock:
            if "\r" in s:
                # Text after the last \r determines
                # whether a bar is on screen.
                after_cr = s.rsplit("\r", 1)[1]
                if after_cr.strip():
                    # Visible bar content → pause
                    self._paused = True
                else:
                    # Only whitespace after \r → bar cleared
                    self._paused = False
            if "\n" in s:
                self._paused = False

    def _write_frame(self, rendered_text: str):
        """Write a single frame to the terminal.

        Moves cursor up to the animated line,
        rewrites it, then moves back down.

        """
        out = self._original_stdout
        with self._lock:
            n = self._lines_below
        # Always move up: +1 accounts for the newline
        # printed by start() after the shimmer line.
        up = n + 1
        buf = [
            SAVE_CURSOR,
            f"\033[{up}A",
            f"\r{self._prefix}{rendered_text}{self._suffix}{CLEAR_LINE_RIGHT}",
            RESTORE_CURSOR,
        ]
        out.write("".join(buf))
        out.flush()

    def _render_frame(self, center: float) -> str:
        """Render one frame with a bright window centered at *center*."""
        chars = []
        half = self._width / 2
        for i, ch in enumerate(self._text):
            dist = abs(i - center)
            if dist < half:
                brightness = math.cos(dist / half * (math.pi / 2))
                if brightness > 0.3:
                    chars.append(f"{BOLD}{ch}{RESET}")
                else:
                    chars.append(ch)
            else:
                chars.append(ch)
        return "".join(chars)

    def _animate(self):
        """Animation loop running in a background thread."""
        n = len(self._text)
        if n == 0:
            return

        sweep_start = -self._width
        sweep_end = n + self._width
        sweep_range = sweep_end - sweep_start
        pos = 0.0
        speed = 0.8  # characters per frame

        while not self._stop_event.is_set():
            with self._lock:
                paused = self._paused
            if not paused:
                center = sweep_start + (pos % sweep_range)
                frame = self._render_frame(center)
                self._write_frame(frame)
                pos += speed
            self._stop_event.wait(self._interval)

    def __enter__(self):
        """Start shimmer as context manager."""
        self.start()
        return self

    def __exit__(self, *args):
        """Stop shimmer when exiting context manager."""
        self.stop()

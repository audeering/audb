"""Shimmer text animation for terminal output."""

from __future__ import annotations

import math
import sys
import threading


# ANSI escape codes
BOLD = "\033[1m"
RESET = "\033[0m"
SAVE_CURSOR = "\033[s"
RESTORE_CURSOR = "\033[u"
CLEAR_LINE_RIGHT = "\033[K"

# Global lock and reference for enforcing single active Shimmer
_active_lock = threading.Lock()
_active_shimmer: Shimmer | None = None


class Shimmer:
    """Animate text with a bright shimmer sweeping across.

    The shimmer highlights a moving window of characters
    in bold/bright while the rest stays in the default color.
    The animation runs in a background thread and uses
    ANSI cursor movement to update the animated line
    even as new content appears below it.

    Args:
        prefix: static text before the animated portion
        text: the text to animate (e.g. the database name).
        suffix: static text after the animated portion
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
        self._original_stdout_write = None
        self._original_stderr_write = None
        self._noop = False

    def start(self):
        """Start the shimmer animation in a background thread.

        The animation is silently skipped (no-op) when:

        * ``sys.stdout`` is not a TTY
          (e.g. redirected output, Jupyter, CI logs).
        * Another ``Shimmer`` instance is already active
          (only one may run at a time).

        """
        global _active_shimmer

        # Skip animation in non-interactive environments
        if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
            sys.stdout.write(f"{self._prefix}{self._text}{self._suffix}\n")
            sys.stdout.flush()
            self._noop = True
            return

        # Enforce single active Shimmer
        with _active_lock:
            if _active_shimmer is not None:
                sys.stdout.write(f"{self._prefix}{self._text}{self._suffix}\n")
                sys.stdout.flush()
                self._noop = True
                return
            _active_shimmer = self

        # Print the initial static line with a newline
        # so subsequent output appears below it.
        sys.stdout.write(f"{self._prefix}{self._text}{self._suffix}\n")
        sys.stdout.flush()
        # Monkey-patch write() on the existing stream objects
        # so all other attributes (fileno, encoding, isatty, …) stay intact.
        self._original_stdout_write = sys.stdout.write
        self._original_stderr_write = sys.stderr.write
        sys.stdout.write = self._stdout_write_hook
        sys.stderr.write = self._stderr_write_hook
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the animation and restore streams."""
        global _active_shimmer

        if self._noop:
            return

        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        # Restore original write methods
        if self._original_stdout_write is not None:
            sys.stdout.write = self._original_stdout_write
        if self._original_stderr_write is not None:
            sys.stderr.write = self._original_stderr_write
        # Write final static line over the animated one
        self._write_frame(self._text)

        with _active_lock:
            if _active_shimmer is self:
                _active_shimmer = None

    def _stdout_write_hook(self, s: str) -> int:
        """Wrap stdout.write to track newlines."""
        count = s.count("\n")
        if count:
            with self._lock:
                self._lines_below += count
        return self._original_stdout_write(s)

    def _stderr_write_hook(self, s: str) -> int:
        r"""Wrap stderr.write to detect progress-bar activity.

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
            if "\n" in s:  # pragma: no cover
                self._paused = False
        return self._original_stderr_write(s)

    def _write_frame(self, rendered_text: str):
        """Write a single frame to the terminal.

        Moves cursor up to the animated line,
        rewrites it, then moves back down.

        """
        write = self._original_stdout_write or sys.stdout.write
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
            write("".join(buf))
            sys.stdout.flush()

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
        if n == 0:  # pragma: no cover
            return

        sweep_start = -self._width
        sweep_end = n + self._width
        sweep_range = sweep_end - sweep_start
        pos = 0.0
        speed = 0.8  # characters per frame

        was_paused = False
        while not self._stop_event.is_set():
            with self._lock:
                paused = self._paused
            if not paused:
                center = sweep_start + (pos % sweep_range)
                frame = self._render_frame(center)
                self._write_frame(frame)
                pos += speed
                was_paused = False
            elif not was_paused:
                # Write plain text on first paused frame
                # to clear any leftover bold highlighting.
                self._write_frame(self._text)
                was_paused = True
            self._stop_event.wait(self._interval)

    def __enter__(self):  # pragma: no cover
        """Start shimmer as context manager."""
        self.start()
        return self

    def __exit__(self, *args):  # pragma: no cover
        """Stop shimmer when exiting context manager."""
        self.stop()

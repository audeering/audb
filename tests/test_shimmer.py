import io

from audb.core.shimmer import BOLD
from audb.core.shimmer import RESET
from audb.core.shimmer import Shimmer
from audb.core.shimmer import _StreamProxy


def test_stream_proxy_isatty():
    """Ensure _StreamProxy delegates isatty() to the target stream.

    tqdm checks isatty() to decide whether to render progress bars.
    If the proxy returns False for a TTY target, bars silently vanish.

    """

    class FakeTTY(io.StringIO):
        def isatty(self):
            return True

    class FakeNonTTY(io.StringIO):
        def isatty(self):
            return False

    proxy_tty = _StreamProxy(FakeTTY(), on_write=lambda s: None)
    assert proxy_tty.isatty() is True

    proxy_non_tty = _StreamProxy(FakeNonTTY(), on_write=lambda s: None)
    assert proxy_non_tty.isatty() is False


def test_stream_proxy_fileno():
    """Ensure _StreamProxy delegates fileno() to the target stream."""

    class FakeStream(io.StringIO):
        def fileno(self):
            return 42

    proxy = _StreamProxy(FakeStream(), on_write=lambda s: None)
    assert proxy.fileno() == 42


def test_stream_proxy_writable():
    """Ensure _StreamProxy reports as writable.

    Code that checks writable() before writing
    would skip the proxy otherwise.

    """
    proxy = _StreamProxy(io.StringIO(), on_write=lambda s: None)
    assert proxy.writable() is True


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

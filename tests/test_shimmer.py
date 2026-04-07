import io

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


def test_stream_proxy_writable():
    """Ensure _StreamProxy reports as writable.

    Code that checks writable() before writing
    would skip the proxy otherwise.

    """
    proxy = _StreamProxy(io.StringIO(), on_write=lambda s: None)
    assert proxy.writable() is True

class Repository:
    r"""Repository object.

    It stores all information
    needed to address a repository:
    the repository name,
    host,
    and backend.

    Args:
        name: repository name
        host: repository host
        backend: repository backend

    Example:
        >>> Repository('data-local', '/data', 'file-system')
        Repository('data-local', '/data', 'file-system')

    """
    def __init__(
            self,
            name: str,
            host: str,
            backend: str,
    ):
        self.name = name
        r"""Repository name."""
        self.host = host
        r"""Repository host."""
        self.backend = backend
        r"""Repository backend."""

    def __repr__(self):
        return (
            f"Repository("
            f"'{self.name}', "
            f"'{self.host}', "
            f"'{self.backend}'"
            f")"
        )

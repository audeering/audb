from doctest import ELLIPSIS
from doctest import NORMALIZE_WHITESPACE
import os

import pytest
import sybil
from sybil.parsers.rest import DocTestParser
from sybil.parsers.rest import PythonCodeBlockParser
from sybil.parsers.rest import SkipParser

import audb
from audb.core.conftest import cache  # noqa: F401
from audb.core.conftest import imports
from audb.core.conftest import public_repository  # noqa: F401


class DefaultConfiguration:
    """Context manager to provide default configuration values."""

    def __init__(self):
        self.config = audb.core.config.load_configuration_file(
            audb.core.config.global_config_file
        )
        self._saved_state = {}

    def __enter__(self):
        """Save current state."""
        self._saved_state = {
            "cache_root": audb.config.CACHE_ROOT,
            "shared_cache_root": audb.config.SHARED_CACHE_ROOT,
            "repositories": audb.config.REPOSITORIES,
        }
        # Set default values
        audb.config.CACHE_ROOT = self.config["cache_root"]
        audb.config.SHARED_CACHE_ROOT = self.config["shared_cache_root"]
        audb.config.REPOSITORIES = [
            audb.Repository(repo["name"], repo["host"], repo["backend"])
            for repo in self.config["repositories"]
        ]

    def __exit__(self, *args):
        """Restore previous state."""
        for key, value in self._saved_state.items():
            setattr(audb.config, key, value)


@pytest.fixture(scope="module")
def default_configuration():
    """Set config values to default values."""
    with DefaultConfiguration():
        yield


@pytest.fixture(scope="module")
def run_in_tmpdir(tmpdir_factory):
    """Move to a persistent tmpdir for execution of a whole file."""
    tmpdir = tmpdir_factory.mktemp("tmp")
    current_dir = os.getcwd()
    os.chdir(tmpdir)

    yield

    os.chdir(current_dir)


# Collect doctests
#
# We use several `sybil.Sybil` instances
# to pass different fixtures for different files
#
parsers = [
    DocTestParser(optionflags=ELLIPSIS + NORMALIZE_WHITESPACE),
    PythonCodeBlockParser(),
    SkipParser(),
]
pytest_collect_file = sybil.sybil.SybilCollection(
    (
        sybil.Sybil(
            parsers=parsers,
            filenames=[
                "authentication.rst",
                "overview.rst",
                "quickstart.rst",
                "dependencies.rst",
                "load.rst",
                "audb.info.rst",
            ],
            fixtures=[
                "cache",
                "run_in_tmpdir",
                "public_repository",
            ],
            setup=imports,
        ),
        sybil.Sybil(
            parsers=parsers,
            filenames=["publish.rst"],
            fixtures=["cache", "run_in_tmpdir"],
            setup=imports,
        ),
        sybil.Sybil(
            parsers=parsers,
            filenames=["configuration.rst", "caching.rst"],
            fixtures=["default_configuration"],
            setup=imports,
        ),
    )
).pytest()

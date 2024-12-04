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


@pytest.fixture(scope="module")
def run_in_tmpdir(tmpdir_factory):
    """Move to a persistent tmpdir for execution of a whole file."""
    tmpdir = tmpdir_factory.mktemp("tmp")
    current_dir = os.getcwd()
    os.chdir(tmpdir)

    yield

    os.chdir(current_dir)


@pytest.fixture(scope="module")
def default_configuration():
    """Set config values to default values from global config file."""
    # Read global config file
    global_config = audb.core.config.load_configuration_file(
        audb.core.config.global_config_file
    )
    # Store current user settings
    current_cache_root = audb.config.CACHE_ROOT
    current_shared_cache_root = audb.config.SHARED_CACHE_ROOT
    current_repositories = audb.config.REPOSITORIES
    # Enforce default values
    audb.config.CACHE_ROOT = global_config["cache_root"]
    audb.config.SHARED_CACHE_ROOT = global_config["shared_cache_root"]
    audb.config.REPOSITORIES = [
        audb.Repository(r["name"], r["host"], r["backend"])
        for r in global_config["repositories"]
    ]

    yield audb

    # Restore user settings
    audb.config.CACHE_ROOT = current_cache_root
    audb.config.SHARED_CACHE_ROOT = current_shared_cache_root
    audb.config.REPOSITORIES = current_repositories


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

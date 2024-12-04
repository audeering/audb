import os
import pathlib
import platform

import pytest

import audeer

import audb


def set_or_delete_env_variable(key, value):
    """Set or delete environment variable.

    Args:
        key: name of environment variable
        value: value of environment variable.
            If ``None``,
            ``key`` is deleted

    """
    if value is None:
        del os.environ[key]
    else:
        os.environ[key] = value


@pytest.fixture()
def config_files(tmpdir, request):
    """Provide user config files.

    The config file at ``~/.config/audb.yaml``
    sets ``cache_root`` to ``~/user1``.
    The config file at ``~/.audb.yam``
    sets ``cache_root`` to ``~/user2``.

    Args:
        tmpdir: tmpdir fixture.
            The tmpdir is used as the home folder
            for storing user config files
        request: request fixture
            for selecting which user config file to create.
            ``"default"`` will create a file at ``~/.config/audb.yaml``,
            ``"deprecated"`` will create a file at ``~/.audb.yaml``,
            ``"both"`` will create both files

    """
    home = audeer.mkdir(tmpdir)
    if platform.system() == "Windows":
        current_homedrive = os.environ.get("HOMEDRIVE", None)
        current_homepath = os.environ.get("HOMEPATH", None)
        homedrive, homepath = os.path.splitdrive(home)
        os.environ["HOMEDRIVE"] = homedrive
        os.environ["HOMEPATH"] = homepath
        print(f"{audeer.path('~')=}")
        print(f"{pathlib.Path.home()=}")
        print(f"{pathlib.Path.home().as_posix()=}")
    else:
        current_home = os.environ.get("HOME", None)
        os.environ["HOME"] = home
    print(f"{home=}")
    if platform.system() == "Windows":
        print(f"{os.environ.get('HOMEDRIVE')=}")
        print(f"{os.environ.get('HOMEPATH')=}")
    if request.param in ["both", "default"]:
        audeer.mkdir(home, ".config")
        with open(audeer.path(home, ".config", "audb.yaml"), "w") as fp:
            fp.write("cache_root: ~/user1\n")
    if request.param in ["both", "deprecated"]:
        with open(audeer.path(home, ".audb.yaml"), "w") as fp:
            fp.write("cache_root: ~/user2\n")

    yield

    if platform.system() == "Windows":
        set_or_delete_env_variable("HOMEDRIVE", current_homedrive)
        set_or_delete_env_variable("HOMEPATH", current_homepath)
    else:
        set_or_delete_env_variable("HOME", current_home)


@pytest.mark.parametrize(
    "config_files, expected",
    [
        ("both", "~/user1"),
        ("default", "~/user1"),
        ("deprecated", "~/user2"),
    ],
    indirect=["config_files"],
)
def test_deprecated_config_file(config_files, expected):
    """Test loading of user configuration files.

    It especially checks,
    if the default configuration file
    overwrites a deprecated user config file.

    Args:
        config_files: config_files fixture
        expected: expected value for ``cache_root``
            config entry

    """
    config = audb.core.config.load_config()
    assert config["cache_root"] == expected

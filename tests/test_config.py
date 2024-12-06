import pytest
import yaml

import audeer

import audb


@pytest.fixture()
def config_files(tmpdir, request):
    """Provide user config files.

    The config file at ``.config/audb.yaml``
    sets ``cache_root`` to ``~/user1``.
    The config file at ``.audb.yam``
    sets ``cache_root`` to ``~/user2``.

    Args:
        tmpdir: tmpdir fixture.
            The tmpdir is used as the home folder
            for storing user config files
        request: request fixture
            for selecting which user config file to create.
            ``"default"`` will create a file at ``tmpdir/.config/audb.yaml``,
            ``"deprecated"`` will create a file at ``tmpdir/.audb.yaml``,
            ``"both"`` will create both files

    """
    home = audeer.mkdir(tmpdir)
    current_user_config_file = audb.core.define.USER_CONFIG_FILE
    current_deprecated_user_config_file = audb.core.define.DEPRECATED_USER_CONFIG_FILE
    audb.core.config.USER_CONFIG_FILE = audeer.path(home, ".config", "audb.yaml")
    audb.core.config.DEPRECATED_USER_CONFIG_FILE = audeer.path(home, ".audb.yaml")
    if request.param in ["both", "default"]:
        audeer.mkdir(home, ".config")
        with open(audeer.path(home, ".config", "audb.yaml"), "w") as fp:
            fp.write("cache_root: ~/user1\n")
    if request.param in ["both", "deprecated"]:
        with open(audeer.path(home, ".audb.yaml"), "w") as fp:
            fp.write("cache_root: ~/user2\n")

    yield

    audb.core.config.USER_CONFIG_FILE = current_user_config_file
    audb.core.config.DEPRECATED_USER_CONFIG_FILE = current_deprecated_user_config_file


def test_config_file(tmpdir):
    root = audeer.mkdir(tmpdir)

    config_file = audeer.path(root, ".audb.yaml")

    # Try loading non-existing file
    config = audb.core.config.load_configuration_file(config_file)
    assert config == {}

    # Add a custom cache entry
    # and check if combining with global config works
    with open(config_file, "w") as cf:
        cf.write("cache_root: ~/user\n")

    config = audb.core.config.load_configuration_file(config_file)
    assert config == {"cache_root": "~/user"}

    global_config = audb.core.config.load_configuration_file(
        audb.core.config.global_config_file
    )
    global_config.update(config)
    assert global_config["cache_root"] == "~/user"
    assert global_config["shared_cache_root"] == "/data/audb"

    # Fail for wrong repositories entries
    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
    error_msg = (
        "You cannot specify an empty 'repositories:' section "
        f"in the configuration file '{audb.core.define.USER_CONFIG_FILE}'."
    )
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
    error_msg = "You cannot specify an empty 'repositories:' section"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
        cf.write("  - host: some-host\n")
        cf.write("    backend: some-backend\n")
    error_msg = "Your repository is missing a 'name' entry"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
        cf.write("  - name: my-repo\n")
    error_msg = "Your repository is missing a 'host' entry."
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
        cf.write("  - name: my-repo\n")
        cf.write("    host: some-host\n")
    error_msg = "Your repository is missing a 'backend' entry"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    # Load custom repository
    with open(config_file, "w") as cf:
        cf.write("repositories:\n")
        cf.write("  - name: my-repo\n")
        cf.write("    host: some-host\n")
        cf.write("    backend: some-backend\n")
    config = audb.core.config.load_configuration_file(config_file)
    assert config == {
        "repositories": [
            {
                "name": "my-repo",
                "host": "some-host",
                "backend": "some-backend",
            },
        ]
    }


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


def test_empty_config_file(tmp_path):
    """Test loading an empty config file."""
    empty_config = tmp_path / "empty.yaml"
    empty_config.write_text("")
    config = audb.core.config.load_configuration_file(empty_config)
    assert config == {}


def test_invalid_config_file(tmp_path):
    """Test loading a broken config file."""
    invalid_config = tmp_path / "invalid.yaml"
    invalid_config.write_text("{invalid: yaml: content}")
    with pytest.raises(yaml.YAMLError):
        audb.core.config.load_configuration_file(invalid_config)

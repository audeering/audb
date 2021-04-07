import os

import pytest

import audb
import audeer


def test_config_file():

    audeer.mkdir(pytest.ROOT)

    config_file = os.path.join(pytest.ROOT, '.audb.yaml')

    # Try loading non-existing file
    config = audb.core.config.load_configuration_file(config_file)
    assert config == {}

    # Add a custom cache entry
    # and check if combining with global config works
    with open(config_file, 'w') as cf:
        cf.write('cache_root: ~/user\n')

    config = audb.core.config.load_configuration_file(config_file)
    assert config == {'cache_root': '~/user'}

    global_config = audb.core.config.load_configuration_file(
        audb.core.config.global_config_file
    )
    global_config.update(config)
    assert global_config['cache_root'] == '~/user'
    assert global_config['shared_cache_root'] == audb.config.SHARED_CACHE_ROOT

    # Fail for wrong repositories entries
    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
    error_msg = (
        "You cannot specify an empty 'repositories:' section "
        f"in the configuration file '{audb.core.define.USER_CONFIG_FILE}'."
    )
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
    error_msg = "You cannot specify an empty 'repositories:' section"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
        cf.write('  - host: some-host\n')
        cf.write('    backend: some-backend\n')
    error_msg = "Your repository is missing a 'name' entry"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
        cf.write('  - name: my-repo\n')
    error_msg = "Your repository is missing a 'host' entry."
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
        cf.write('  - name: my-repo\n')
        cf.write('    host: some-host\n')
    error_msg = "Your repository is missing a 'backend' entry"
    with pytest.raises(ValueError, match=error_msg):
        audb.core.config.load_configuration_file(config_file)

    # Load custom repository
    with open(config_file, 'w') as cf:
        cf.write('repositories:\n')
        cf.write('  - name: my-repo\n')
        cf.write('    host: some-host\n')
        cf.write('    backend: some-backend\n')
    config = audb.core.config.load_configuration_file(config_file)
    assert config == {
        'repositories': [
            {
                'name': 'my-repo',
                'host': 'some-host',
                'backend': 'some-backend',
            },
        ]
    }

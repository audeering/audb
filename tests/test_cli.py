import pytest


@pytest.mark.script_launch_mode('subprocess')
@pytest.mark.parametrize(
    'name,version,table',
    [
        (
            'emodb',
            '1.0.1',
            'emotion',
        ),
    ],
)
def test_audb2(script_runner, name, table, version):

    ret = script_runner.run(
        'audb2', 'load', name,
        '--version', version,
        '--only_metadata', 'True',
        '--verbose', 'False',
    )
    assert ret.success
    assert ret.stderr == ''

    ret = script_runner.run(
        'audb2get', name, table,
        '--version', version,
        '--only_metadata', 'True',
        '--verbose', 'False',
    )
    assert ret.success
    assert ret.stderr == ''

    ret = script_runner.run(
        'audb2info', 'description', name, '--version', version,
    )
    assert ret.success
    assert ret.stderr == ''

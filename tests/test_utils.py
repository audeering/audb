import os

import pytest

import audeer

import audb


@pytest.mark.parametrize(
    'files, expected',
    [
        (
            ['f', os.path.join('sub', 'f')],
            'b540f38948f445622adc657a757f4b0d',
        ),
        (
            ['f', os.path.join('sub', 'g')],
            '305107efbb15f9334d22ae4fbeec4de6',
        ),
        (
            ['ä', 'ö', 'ü', 'ß'],
            '622165ad36122984c6b2c7ba466aa262',
        ),
    ]
)
def test_md5(tmpdir, files, expected):

    root = tmpdir

    for file in files:
        path = audeer.path(root, file)
        audeer.mkdir(os.path.dirname(path))
        audeer.touch(path)

    assert audb.core.utils.md5(root) == expected

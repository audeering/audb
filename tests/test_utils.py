import os

import pytest

import audeer

import audb


@pytest.mark.parametrize(
    'files, expected',
    [
        (
            ['f', os.path.join('sub', 'f')],
            'b7a818f20a169f8e903408706fdbb2cb',
        ),
        (
            ['f', os.path.join('sub', 'g')],
            '234b7950b37a3ff61d4d422233c65347',
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

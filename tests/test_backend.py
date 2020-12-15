import os

import pytest

import audeer

import audb2


@pytest.mark.parametrize(
    'files, name, group, version, force',
    [
        (
            [],
            'empty',
            None,
            '1.0.0',
            False,
        ),
        (
            'file.ext',
            'not-empty',
            None,
            '1.0.0',
            False,
        ),
        (
            ['file.ext', os.path.join('dir', 'to', 'file.ext')],
            'not-empty',
            'group',
            '1.0.0',
            False,
        ),
        (
            ['file.ext', os.path.join('dir', 'to', 'file.ext')],
            'not-empty',
            'group',
            '1.0.0',
            True,
        ),
        pytest.param(
            ['file.ext', os.path.join('dir', 'to', 'file.ext')],
            'not-empty',
            'group',
            '1.0.0',
            False,
            marks=pytest.mark.xfail(raises=FileExistsError),
        ),
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(
            'test_put_archive',
            pytest.HOST,
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
        audb2.backend.Artifactory(
            'test_put_archive',
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
    ]
)
def test_archive(tmpdir, files, name, group, version, force, backend):
    files_as_list = [files] if isinstance(files, str) else files
    for file in files_as_list:
        path = os.path.join(tmpdir, file)
        audeer.mkdir(os.path.dirname(path))
        with open(path, 'w'):
            pass
    backend.put_archive(
        tmpdir, files, name, version, group=group, force=force,
    )
    assert backend.exists(tmpdir, name + '.zip', version, group=group)
    assert backend.get_archive(
        tmpdir, name, version, group=group,
    ) == files_as_list


@pytest.mark.parametrize(
    'file, name, group, version, force',
    [
        (
            'file.ext',
            None,
            None,
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            None,
            None,
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'alias',
            None,
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'alias',
            'group',
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'alias',
            'group',
            '1.0.0',
            True,
        ),
        pytest.param(
            'dir/to/file.ext',
            'alias',
            'group',
            '1.0.0',
            False,
            marks=pytest.mark.xfail(raises=FileExistsError),
        )
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(
            'test_file',
            pytest.HOST,
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
        audb2.backend.Artifactory(
            'test_file',
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
    ]
)
def test_file(tmpdir, file, name, group, version, force, backend):
    path = os.path.join(tmpdir, file)
    audeer.mkdir(os.path.dirname(path))
    with open(path, 'w'):
        pass
    backend.put_file(
        tmpdir, file, version, name=name, group=group, force=force,
    )
    assert backend.exists(tmpdir, file, version, name=name, group=group)
    assert path == backend.get_file(
        tmpdir, file, version, name=name, group=group,
    )


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(
            'test_errors',
            pytest.HOST,
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
        audb2.backend.Artifactory(
            'test_errors',
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
    ]
)
def test_errors(tmpdir, backend):
    with pytest.raises(FileNotFoundError):
        backend.put_file(
            tmpdir,
            'does-not-exist',
            '1.0.0',
        )
    with pytest.raises(FileNotFoundError):
        backend.put_archive(
            tmpdir,
            'does-not-exist',
            'archive',
            '1.0.0',
        )
    with pytest.raises(FileNotFoundError):
        backend.get_file(
            tmpdir,
            'does-not-exist',
            '1.0.0',
        )
    with pytest.raises(FileNotFoundError):
        backend.get_archive(
            tmpdir,
            'does-not-exist',
            '1.0.0',
        )


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(
            'test_versions',
            pytest.HOST,
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
        audb2.backend.Artifactory(
            'test_versions',
            repository=pytest.REPOSITORY_PUBLIC,
            group_id=pytest.GROUP_ID,
        ),
    ]
)
def test_versions(tmpdir, backend):
    assert not backend.versions()
    with pytest.raises(RuntimeError):
        backend.latest_version()
    path = os.path.join(tmpdir, 'db.yaml')
    with open(path, 'w'):
        pass
    backend.put_file(
        tmpdir,
        'db.yaml',
        '1.0.0',
    )
    assert backend.versions() == ['1.0.0']
    backend.put_file(
        tmpdir,
        'db.yaml',
        '2.0.0',
    )
    assert backend.versions() == ['1.0.0', '2.0.0']
    assert backend.latest_version() == '2.0.0'

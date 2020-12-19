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
            pytest.HOST,
        ),
        audb2.backend.Artifactory(),
    ]
)
def test_archive(tmpdir, files, name, group, version, force, backend):

    repository = pytest.REPOSITORY_PUBLIC
    group_id = f'{pytest.GROUP_ID}.{group}'

    files_as_list = [files] if isinstance(files, str) else files
    for file in files_as_list:
        path = os.path.join(tmpdir, file)
        audeer.mkdir(os.path.dirname(path))
        with open(path, 'w'):
            pass
    backend.put_archive(
        tmpdir, files, name, version, repository, group_id, force=force,
    )
    assert backend.exists(name + '.zip', version, repository, group_id)
    assert backend.get_archive(
        tmpdir, name, version, repository, group_id,
    ) == files_as_list


@pytest.mark.parametrize(
    'file, name, version, force',
    [
        (
            'file.ext',
            None,
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            None,
            '1.0.0',
            False,
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'alias',
            '1.0.0',
            False,
        ),
        pytest.param(
            'dir/to/file.ext',
            'alias',
            '1.0.0',
            False,
            marks=pytest.mark.xfail(raises=FileExistsError),
        )
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_file(tmpdir, file, name, version, force, backend):

    repository = pytest.REPOSITORY_PUBLIC
    group_id = f'{pytest.GROUP_ID}.test_file'

    path = os.path.join(tmpdir, file)
    audeer.mkdir(os.path.dirname(path))
    with open(path, 'w'):
        pass
    backend.put_file(
        tmpdir, file, version, repository, group_id, name=name, force=force,
    )
    assert backend.exists(file, version, repository, group_id, name=name)
    assert path == backend.get_file(
        tmpdir, file, version, repository, group_id, name=name,
    )


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_errors(tmpdir, backend):

    repository = pytest.REPOSITORY_PUBLIC
    group_id = f'{pytest.GROUP_ID}.test_errors'

    with pytest.raises(FileNotFoundError):
        backend.put_file(
            tmpdir,
            'does-not-exist',
            '1.0.0',
            repository,
            group_id,
        )
    with pytest.raises(FileNotFoundError):
        backend.put_archive(
            tmpdir,
            'does-not-exist',
            'archive',
            '1.0.0',
            repository,
            group_id,
        )
    with pytest.raises(FileNotFoundError):
        backend.get_file(
            tmpdir,
            'does-not-exist',
            '1.0.0',
            repository,
            group_id,
        )
    with pytest.raises(FileNotFoundError):
        backend.get_archive(
            tmpdir,
            'does-not-exist',
            '1.0.0',
            repository,
            group_id,
        )


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_versions(tmpdir, backend):

    repository = pytest.REPOSITORY_PUBLIC
    group_id = f'{pytest.GROUP_ID}.test_versions'
    file = 'db.yaml'

    assert not backend.versions(file, repository, group_id)
    with pytest.raises(RuntimeError):
        backend.latest_version(file, repository, group_id)
    path = os.path.join(tmpdir, file)
    with open(path, 'w'):
        pass
    backend.put_file(tmpdir, file, '1.0.0', repository, group_id)
    assert backend.versions(file, repository, group_id) == ['1.0.0']
    backend.put_file(tmpdir, file, '2.0.0', repository, group_id)
    assert backend.versions(file, repository, group_id) == ['1.0.0', '2.0.0']
    assert backend.latest_version(file, repository, group_id) == '2.0.0'

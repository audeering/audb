import os

import pytest

import audeer

import audb2


@pytest.mark.parametrize(
    'files, name, folder, version',
    [
        (
            [],
            'empty',
            None,
            '1.0.0',
        ),
        (
            'file.ext',
            'not-empty',
            None,
            '1.0.0',
        ),
        (
            ['file.ext', os.path.join('dir', 'to', 'file.ext')],
            'not-empty',
            'group',
            '1.0.0',
        ),
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_archive(tmpdir, files, name, folder, version, backend):

    repository = pytest.REPOSITORY

    files_as_list = [files] if isinstance(files, str) else files
    for file in files_as_list:
        path = os.path.join(tmpdir, file)
        audeer.mkdir(os.path.dirname(path))
        with open(path, 'w'):
            pass

    archive = backend.join(
        pytest.ID,
        'test_archive',
        name,
    )
    path_backend = backend.put_archive(
        tmpdir, files, archive, version, repository,
    )
    assert backend.put_archive(  # operation will be skipped
        tmpdir, files, archive, version, repository,
    ) == path_backend
    assert backend.exists(archive + '.zip', version, repository)

    assert backend.get_archive(
        archive, tmpdir, version, repository,
    ) == files_as_list


@pytest.mark.parametrize(
    'local_file, remote_file, version',
    [
        (
            'file.ext',
            'file.ext',
            '1.0.0',
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'dir/to/file.ext',
            '1.0.0',
        ),
        (
            os.path.join('dir', 'to', 'file.ext'),
            'alias',
            '1.0.0',
        ),
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_file(tmpdir, local_file, remote_file, version, backend):

    repository = pytest.REPOSITORY

    local_file = os.path.join(tmpdir, local_file)
    audeer.mkdir(os.path.dirname(local_file))
    with open(local_file, 'w'):
        pass

    remote_file = backend.join(
        pytest.ID,
        'test_file',
        remote_file,
    )

    assert not backend.exists(remote_file, version, repository)
    path_backend = backend.put_file(
        local_file, remote_file, version, repository,
    )
    assert backend.put_file(  # operation will be skipped
        local_file, remote_file, version, repository,
    ) == path_backend
    assert backend.exists(remote_file, version, repository)

    backend.get_file(remote_file, local_file, version, repository)
    assert os.path.exists(local_file)
    assert backend.checksum(
        remote_file, version, repository,
    ) == audb2.core.utils.md5(local_file)

    assert backend.remove_file(
        remote_file, version, repository,
    ) == path_backend
    assert not backend.exists(remote_file, version, repository)


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_errors(tmpdir, backend):

    repository = pytest.REPOSITORY

    file_name = 'does-not-exist'
    local_file = os.path.join(tmpdir, file_name)
    remote_file = backend.join(
        pytest.ID,
        'test_errors',
        file_name,
    )

    with pytest.raises(FileNotFoundError):
        backend.put_file(
            local_file,
            remote_file,
            '1.0.0',
            repository,
        )
    with pytest.raises(FileNotFoundError):
        backend.put_archive(
            tmpdir,
            'archive',
            remote_file,
            '1.0.0',
            repository,
        )
    with pytest.raises(FileNotFoundError):
        backend.get_file(
            remote_file,
            local_file,
            '1.0.0',
            repository,
        )
    with pytest.raises(FileNotFoundError):
        backend.get_archive(
            remote_file,
            tmpdir,
            '1.0.0',
            repository,
        )
    with pytest.raises(FileNotFoundError):
        backend.checksum(
            remote_file,
            '1.0.0',
            repository,
        )
    with pytest.raises(FileNotFoundError):
        backend.remove_file(
            remote_file,
            '1.0.0',
            repository,
        )


@pytest.mark.parametrize(
    'files',
    [
        [],
        ['file.ext', os.path.join('path', 'to', 'file.ext')],
    ],
)
@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_glob(tmpdir, files, backend):

    repository = pytest.REPOSITORY

    paths = []
    for file in files:
        local_file = os.path.join(tmpdir, file)
        audeer.mkdir(os.path.dirname(local_file))
        with open(local_file, 'w'):
            pass
        remote_file = backend.join(
            pytest.ID,
            'test_glob',
            file,
        )
        paths.append(
            backend.put_file(local_file, remote_file, '1.0.0', repository)
        )

    pattern = f'{pytest.ID}/test_glob/**/*.ext'
    assert set(paths) == set(backend.glob(pattern, repository))


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
def test_versions(tmpdir, backend):

    repository = pytest.REPOSITORY

    file_name = 'db.yaml'
    local_file = os.path.join(tmpdir, file_name)
    with open(local_file, 'w'):
        pass
    remote_file = backend.join(
        pytest.ID,
        'test_versions',
        file_name,
    )

    assert not backend.versions(remote_file, repository)
    backend.put_file(local_file, remote_file, '1.0.0', repository)
    assert backend.versions(remote_file, repository) == ['1.0.0']
    backend.put_file(local_file, remote_file, '2.0.0', repository)
    assert backend.versions(remote_file, repository) == ['1.0.0', '2.0.0']


@pytest.mark.parametrize(
    'backend',
    [
        audb2.backend.FileSystem(pytest.HOST),
        audb2.backend.Artifactory(),
    ]
)
@pytest.mark.parametrize(
    'path',
    [
        'media/test1-12.344',
        pytest.param(
            r'media\test1',
            marks=pytest.mark.xfail(raises=ValueError),
        ),
    ]
)
def test_path_names(backend, path):
    backend.path(path, None, pytest.REPOSITORY)

import errno
import hashlib
import os
import typing
import zipfile

import audeer


# replace once https://github.com/audeering/audeer/issues/19 is solved
def create_archive(
        root: str,
        files: typing.Sequence[str],
        out_file: str,
):
    r"""Create archive."""
    out_file = audeer.safe_path(out_file)
    audeer.mkdir(os.path.dirname(out_file))
    with zipfile.ZipFile(out_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            full_file = audeer.safe_path(os.path.join(root, file))
            if not os.path.exists(full_file):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), full_file,
                )
            zf.write(full_file, arcname=file)


def md5_read_chunk(
        fp: typing.IO,
        chunk_size: int = 8192,
):
    while True:
        data = fp.read(chunk_size)
        if not data:
            break
        yield data


def md5(
        file: str,
        chunk_size: int = 8192,
) -> str:
    r"""Create MD5 checksum."""
    file = audeer.safe_path(file)
    with open(file, 'rb') as fp:
        hasher = hashlib.md5()
        for chunk in md5_read_chunk(fp, chunk_size):
            hasher.update(chunk)
        return hasher.hexdigest()


def sort_versions(versions: typing.List[str]):
    r"""Sort versions inplace."""
    versions.sort(key=lambda s: list(map(int, s.split('.'))))


def subdirs(
        root,
        ignore_hidden=False,
) -> typing.Sequence[str]:
    r"""List sub-folders."""
    return [d for d in os.listdir(root)
            if os.path.isdir(os.path.join(root, d))
            and (not ignore_hidden or d[0] != '.')]

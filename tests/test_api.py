import os
import tempfile

import audb
import audeer
import audformat
import audiofile

import numpy as np
import pandas as pd


def test_available(repository):

    # Broken database in repo
    name = 'non-existing-database'
    path = os.path.join(
        repository.host,
        repository.name,
        name,
    )
    path = audeer.mkdir(path)
    df = audb.available()
    os.rmdir(path)
    assert len(df) == 0

    # Non existing repo
    name = 'non-existing-repo'
    audb.config.REPOSITORIES = [
        audb.Repository(
            name=name,
            host=repository.host,
            backend=repository.backend,
        )
    ]
    df = audb.available()
    assert len(df) == 0


def test_media_files_order():
    media_files = {}
    n_iterations = 10
    for i in range(n_iterations):
        with tempfile.TemporaryDirectory() as tmp:
            name = 'db'
            version = '1.0.0'

            host = audeer.mkdir(audeer.path(tmp, 'host'))
            db_root = audeer.mkdir(audeer.path(tmp, name))
            cache_root = audeer.mkdir(audeer.path(tmp, 'cache'))

            sampling_rate = 8000
            duration = 1
            signal = np.zeros((1, duration * sampling_rate))
            audiofile.write(
                audeer.path(db_root, 'f0.wav'), signal, sampling_rate
            )
            audiofile.write(
                audeer.path(db_root, 'f1.wav'), signal, sampling_rate
            )

            db = audformat.Database(name)
            index = audformat.filewise_index(['f0.wav', 'f1.wav'])
            db['table'] = audformat.Table(index)
            db['table']['column'] = audformat.Column()
            db['table']['column'].set(['label0', 'label1'])
            db.save(db_root)

            repository = audb.Repository(
                name='repo',
                host=host,
                backend='file-system',
            )
            audb.config.REPOSITORIES = [repository]
            audb.config.CACHE_ROOT = cache_root

            audb.publish(db_root, version, repository)

            deps = audb.dependencies(name, version=version)

            media_files[i] = deps()

    for i in range(n_iterations - 1):
        pd.testing.assert_frame_equal(media_files[i], media_files[i + 1])

import hashlib
import os
import pickle
import random
import string
import time

import pandas as pd

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")


def active_branch():
    head_dir = audeer.path("..", ".git", "HEAD")
    with open(head_dir, "r") as f:
        content = f.read().splitlines()
    for line in content:
        if line[0:4] == "ref:":
            return line.partition("refs/heads/")[2]


# === Dependency pandas.DataFrame ===
data_cache = audeer.path(cache, "df.pkl")
num_rows = 1000000
dtypes = [str, str, int, int, str, float, str, int, int, int, str]
columns = [
    "file",
    "archive",
    "bit_depth",
    "channels",
    "checksum",
    "duration",
    "format",
    "removed",
    "sampling_rate",
    "type",
    "version",
]
if not os.path.exists(data_cache):
    records = [
        {
            "file": f"file-{n}.wav",
            "archive": f"archive-{n}",
            "bit_depth": random.choices([0, 16, 24], weights=[0.1, 0.8, 0.1])[0],
            "channels": random.choices([0, 1, 2], weights=[0.1, 0.8, 0.1])[0],
            "checksum": hashlib.md5(
                pickle.dumps(random.choice(string.ascii_letters))
            ).hexdigest(),
            "duration": 10 * random.random(),
            "format": random.choices(["csv", "wav", "txt"], weights=[0.1, 0.8, 0.1])[0],
            "removed": random.choices([0, 1], weights=[0.1, 0.9])[0],
            "sampling_rate": random.choices(
                [0, 16000, 44100],
                weights=[0.1, 0.8, 0.1],
            )[0],
            "type": random.choices([0, 1, 2], weights=[0.1, 0.8, 0.1])[0],
            "version": random.choices(["1.0.0", "1.1.0"], weights=[0.2, 0.8])[0],
        }
        for n in range(num_rows)
    ]
    df = pd.DataFrame.from_records(records)
    for column, dtype in zip(df.columns, dtypes):
        df[column] = df[column].astype(dtype)
    df.set_index("file", inplace=True)
    df.index.name = ""
    df.to_pickle(data_cache)

# === Create dependency object ===
deps = audb.Dependencies()
deps.load(data_cache)

# ===== Benchmark audb.Dependency =====
file = "file-10.wav"

t = time.time()
deps()
print(f"Dependency.__call__(): {time.time() -t:.3f} s")

# Access the index one time.
# Further calls will be faster
file in deps

t = time.time()
file in deps
print(f"Dependency.__contains__(): {time.time() -t:.3f} s")

t = time.time()
deps[file]
print(f"Dependency.__get_item__(): {time.time() -t:.3f} s")

t = time.time()
len(deps)
print(f"Dependency.__len__(): {time.time() -t:.3f} s")

t = time.time()
str(deps)
print(f"Dependency.__str__(): {time.time() -t:.3f} s")

t = time.time()
deps.archives
print(f"Dependency.archives: {time.time() -t:.3f} s")

t = time.time()
deps.attachments
print(f"Dependency.attachments: {time.time() -t:.3f} s")

t = time.time()
deps.attachment_ids
print(f"Dependency.attachment_ids: {time.time() -t:.3f} s")

t = time.time()
files = deps.files
print(f"Dependency.files: {time.time() -t:.3f} s")

t = time.time()
deps.media
print(f"Dependency.media: {time.time() -t:.3f} s")

t = time.time()
deps.removed_media
print(f"Dependency.removed_media: {time.time() -t:.3f} s")

t = time.time()
deps.table_ids
print(f"Dependency.table_ids: {time.time() -t:.3f} s")

t = time.time()
deps.tables
print(f"Dependency.tables: {time.time() -t:.3f} s")

n_files = 1000
_files = files[:n_files]

t = time.time()
[deps.archive(file) for file in _files]
print(f"Dependency.archive({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.bit_depth(file) for file in _files]
print(f"Dependency.bit_depth({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.channels(file) for file in _files]
print(f"Dependency.channels({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.checksum(file) for file in _files]
print(f"Dependency.checksum({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.duration(file) for file in _files]
print(f"Dependency.duration({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.format(file) for file in _files]
print(f"Dependency.format({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.removed(file) for file in _files]
print(f"Dependency.removed({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.sampling_rate(file) for file in _files]
print(f"Dependency.sampling_rate({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.type(file) for file in _files]
print(f"Dependency.type({n_files} files): {time.time() -t:.3f} s")

t = time.time()
[deps.version(file) for file in _files]
print(f"Dependency.version({n_files} files): {time.time() -t:.3f} s")

# -------------------------------------------------------------------------
t = time.time()
deps._add_attachment("attachment.txt", "1.0.0", "archive", "checksum")
print(f"Dependency._add_attachment(): {time.time() -t:.3f} s")

if active_branch() == "deps-parquet":
    values = [
        (
            f"file-new-{n}.wav",  # file
            f"archive-new-{n}",  # archive
            16,  # bit_depth
            1,  # channels
            f"checksum-{n}",  # checksum
            0.4,  # duration
            16000,  # sampling_rate
            "1.0.0",  # version
        )
        for n in range(n_files)
    ]
else:
    values = [
        (
            f"file-new-{n}.wav",  # file
            f"archive-new-{n}",  # archive
            16,  # bit_depth
            1,  # channels
            f"checksum-{n}",  # checksum
            0.4,  # duration
            "wav",  # format
            0,  # removed
            16000,  # sampling_rate
            1,  # type
            "1.0.0",  # version
        )
        for n in range(n_files)
    ]

t = time.time()
deps._add_media(values)
print(f"Dependency._add_media({n_files} files): {time.time() -t:.3f} s")

t = time.time()
deps._add_meta("db.new-table.csv", "1.0.0", "archive", "checksum")
print(f"Dependency._add_meta(): {time.time() -t:.3f} s")

t = time.time()
deps._drop("file-9000.wav")
print(f"Dependency._drop(): {time.time() -t:.3f} s")

t = time.time()
deps._remove(file)
print(f"Dependency._remove(): {time.time() -t:.3f} s")

t = time.time()
deps._update_media(values)
print(f"Dependency._update_media(): {time.time() -t:.3f} s")

t = time.time()
deps._update_media_version([f"file-{n}.wav" for n in range(n_files)], "version")
print(f"Dependency._update_media_version({n_files} files): {time.time() -t:.3f} s")

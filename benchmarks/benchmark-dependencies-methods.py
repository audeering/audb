# /// script
# dependencies = [
#   "audb",
#   "tabulate",
# ]
#
# [tool.uv.sources]
# audb = { path = "../", editable = true }
# ///

import hashlib
import os
import pickle
import random
import string
import time

import pandas as pd
import tabulate

import audeer

import audb


random.seed(1)

cache = audeer.mkdir("./cache")

print(f"audb v{audb.__version__}")


# === Create legacy CSV dependency table ===
data_cache = audeer.path(cache, "df.csv")
num_rows = 1000000
if not os.path.exists(data_cache):
    bit_depths = [0, 16, 24]
    channels = [0, 1, 2]
    formats = ["csv", "wav", "txt"]
    sampling_rates = [0, 16000, 44100]
    types = [0, 1, 2]
    versions = ["1.0.0", "1.1.0"]
    records = [
        {
            "file": f"file-{n}.wav",
            "archive": f"archive-{n}",
            "bit_depth": random.choices(bit_depths, weights=[0.1, 0.8, 0.1])[0],
            "channels": random.choices(channels, weights=[0.1, 0.8, 0.1])[0],
            "checksum": hashlib.md5(
                pickle.dumps(random.choice(string.ascii_letters))
            ).hexdigest(),
            "duration": 10 * random.random(),
            "format": random.choices(formats, weights=[0.1, 0.8, 0.1])[0],
            "removed": random.choices([0, 1], weights=[0.1, 0.9])[0],
            "sampling_rate": random.choices(sampling_rates, weights=[0.1, 0.8, 0.1])[0],
            "type": random.choices(types, weights=[0.1, 0.8, 0.1])[0],
            "version": random.choices(versions, weights=[0.2, 0.8])[0],
        }
        for n in range(num_rows)
    ]
    df = pd.DataFrame.from_records(records)
    df = df.astype(audb.core.define.DEPENDENCY_TABLE)
    df.set_index("file", inplace=True)
    df.index.name = None
    df.index = df.index.astype(audb.core.define.DEPENDENCY_INDEX_DTYPE)
    df.to_csv(data_cache)

# Prepare deps object
deps = audb.Dependencies()
deps.load(data_cache)
deps_file = audeer.path(cache, audb.core.define.DEPENDENCY_FILE)
n_files = 10000
_files = deps.files[:n_files]
results = pd.DataFrame(columns=["result"])
results.index.name = "method"

# ===== Benchmark audb.Dependencies =====
method = "Dependencies.save()"
t0 = time.time()
deps.save(deps_file)
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.load()"
deps = audb.Dependencies()
t0 = time.time()
deps.load(deps_file)
t = time.time() - t0
results.at[method, "result"] = t

method = r"Dependencies.\_\_call\_\_()"
t0 = time.time()
deps()
t = time.time() - t0
results.at[method, "result"] = t

# Pre-warm accesses
# Further calls will be faster
"file-10.wav" in deps
deps.archives
_ = deps.archive(_files[0])

method = rf"Dependencies.\_\_contains\_\_({n_files} files)"
t0 = time.time()
for file in _files:
    _ = file in deps
t = time.time() - t0
results.at[method, "result"] = t

method = rf"Dependencies.\_\_get_item\_\_({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps[file]
t = time.time() - t0
results.at[method, "result"] = t

method = r"Dependencies.\_\_len\_\_()"
t0 = time.time()
len(deps)
t = time.time() - t0
results.at[method, "result"] = t

method = r"Dependencies.\_\_str\_\_()"
t0 = time.time()
str(deps)
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.archives"
t0 = time.time()
deps.archives
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.attachments"
t0 = time.time()
deps.attachments
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.attachment_ids"
t0 = time.time()
deps.attachment_ids
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.files"
t0 = time.time()
deps.files
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.media"
t0 = time.time()
deps.media
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.removed_media"
t0 = time.time()
deps.removed_media
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.table_ids"
t0 = time.time()
deps.table_ids
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies.tables"
t0 = time.time()
deps.tables
t = time.time() - t0
results.at[method, "result"] = t

# Pre-warm _df access
_ = deps.archive(_files[0])

method = f"Dependencies.archive({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.archive(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.bit_depth({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.bit_depth(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.channels({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.channels(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.checksum({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.checksum(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.duration({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.duration(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.format({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.format(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.removed({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.removed(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.sampling_rate({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.sampling_rate(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.type({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.type(file)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies.version({n_files} files)"
t0 = time.time()
for file in _files:
    _ = deps.version(file)
t = time.time() - t0
results.at[method, "result"] = t

# -------------------------------------------------------------------------
method = "Dependencies._add_attachment()"
t0 = time.time()
deps._add_attachment("attachment.txt", "1.0.0", "archive", "checksum")
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies._add_media({n_files} files)"
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
t0 = time.time()
deps._add_media(values)
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies._add_meta()"
t0 = time.time()
deps._add_meta("db.new-table.csv", "1.0.0", "checksum")
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies._drop()"
t0 = time.time()
deps._drop(["file-90000.wav"])
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies._remove()"
t0 = time.time()
deps._remove("file-10.wav")
t = time.time() - t0
results.at[method, "result"] = t

method = "Dependencies._update_media()"
t0 = time.time()
deps._update_media(values)
t = time.time() - t0
results.at[method, "result"] = t

method = f"Dependencies._update_media_version({n_files} files)"
t0 = time.time()
deps._update_media_version([f"file-{n}.wav" for n in range(n_files)], "version")
t = time.time() - t0
results.at[method, "result"] = t


# ===== Save results =====
fp_results = audeer.path(cache, "results.csv")
results.to_csv(fp_results)

# ===== Print results =====
table = tabulate.tabulate(results, headers="keys", tablefmt="github", floatfmt=".3f")
print(table)

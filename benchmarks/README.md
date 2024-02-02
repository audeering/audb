# Benchmarks

This folder contains benchmarks
used to evaluate design decisions
in `audb`.

The reported results were calculated
on the following machine:

* CPU: 13th Gen Intel Core i7-1355U, 10-core (2-mt/8-st)
* RAM: 15.29 GiB
* Hard drive: KIOXIA KXG8AZNV1T02
* Linux: Ubuntu 22.04
* Python: 3.10


## Loading and saving dependency files

Dependencies for each dataset
are originally stored in CSV files
on the server
and in pickle files
in the local cache.
They are not supposed to be directly read
by a user.

This benchmark tests
potential gains for

1. read and write CSV files with `pyarrow`
2. represent dependencies as `pyarrow.Table`.
  This includes converting
  `pandas.DataFrame`
  from legacy pickle cache files.
3. store dependencies as parquet files

Execute the benchmark with:

```bash
$ python benchmark-dependencies-save-and-load.py
```

**Reading a dependency file
with 1,000,000 entries
from CSV, pickle, or parquet**

| Destination                       | CSV    | pickle | parquet |
| --------------------------------- | ------ | ------ | ------- |
| pandas.DataFrame                  | 1.15 s | 0.19 s | 0.37 s  |
| pyarrow.Table -> pandas.DataFrame | 0.41 s |        | 0.39 s  |
| pyarrow.Table                     | 0.05 s |        | 0.05 s  |
| pandas.DataFrame -> pyarrow.Table |        | 0.47 s |         |

**Writing a dependency file
with 1,000,000 entries
to CSV, pickle, or parquet

| Origin                            | CSV    | pickle | parquet |
| --------------------------------- | ------ | ------ | ------- |
| pandas.DataFrame                  | 1.96 s | 0.70 s | 0.47 s  |
| pandas.DataFrame -> pyarrow.Table | 0.47 s | 0.77 s |         |
| pyarrow.Table                     | 0.25 s |        | 0.24 s  |


| Method                               | pyarrow.Table | pandas.DataFrame |
| ------------------------------------ | ------------- | ---------------- |
| `Dependency.__call__()`              | 0.353 s       |                  |
| `Dependency.__contains__()`          | 0.002 s       |
| `Dependency.__get_item__()`          | 0.001 s       | 
| `Dependency.__len__()`               | 0.000 s       |
| `Dependency.__str__()`               | 0.003 s       |
| `Dependency.archives`                | 0.779 s       |
| `Dependency.attachments`             | 0.051 s       |
| `Dependency.attachment_ids`          | 0.055 s       |
| `Dependency.files`                   | 0.384 s       |
| `Dependency.media`                   | 0.360 s       |
| `Dependency.removed_media`           | 0.348 s       |
| `Dependency.table_ids`               | 0.059 s       |
| `Dependency.tables`                  | 0.054 s       |
| `Dependency.archive()`               | 0.001 s       |
| `Dependency.bit_depth()`             | 0.001 s       |
| `Dependency.channels()`              | 0.001 s       |
| `Dependency.checksum()`              | 0.002 s       |
| `Dependency.duration()`              | 0.002 s       |
| `Dependency.format()`                | 0.001 s       |
| `Dependency.removed()`               | 0.002 s       |
| `Dependency.sampling_rate()`         | 0.001 s       |
| `Dependency.type()`                  | 0.001 s       |
| `Dependency.version()`               | 0.001 s       |
| `Dependency._add_attachment()`       | 0.125 s       |
| `Dependency._add_media()`            | 0.045 s       |
| `Dependency._add_meta()`             | 0.098 s       |
| `Dependency._drop()`                 | 0.038 s       |
| `Dependency._remove()`               | 0.050 s       |
| `Dependency._update_media()`         | 0.081 s       |
| `Dependency._update_media_version()` | 1.178 s       |


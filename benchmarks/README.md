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
to CSV, pickle, or parquet**

| Origin                            | CSV    | pickle | parquet |
| --------------------------------- | ------ | ------ | ------- |
| pandas.DataFrame                  | 1.96 s | 0.70 s | 0.47 s  |
| pandas.DataFrame -> pyarrow.Table | 0.47 s | 0.77 s |         |
| pyarrow.Table                     | 0.25 s |        | 0.24 s  |


## Parsing dependencies

Compares representing the dependencies internally
as a `pyarrow.Table` object
or as a `pandas.DataFrame`.


| Method                                         | pyarrow.Table | pandas.DataFrame |
| ---------------------------------------------- | ------------- | ---------------- |
| `Dependency.__call__()`                        |       0.315 s |          0.000 s |
| `Dependency.__contains__()`                    |       0.001 s |          0.000 s |
| `Dependency.__get_item__()`                    |       0.001 s |          0.000 s |
| `Dependency.__len__()`                         |       0.000 s |          0.000 s |
| `Dependency.__str__()`                         |       0.006 s |          0.006 s |
| `Dependency.archives`                          |       0.124 s |          0.413 s |
| `Dependency.attachments`                       |       0.019 s |          0.021 s |
| `Dependency.attachment_ids`                    |       0.022 s |          0.022 s |
| `Dependency.files`                             |       0.039 s |          0.029 s |
| `Dependency.media`                             |       0.090 s |          0.094 s |
| `Dependency.removed_media`                     |       0.097 s |          0.092 s |
| `Dependency.table_ids`                         |       0.022 s |          0.030 s |
| `Dependency.tables`                            |       0.018 s |          0.021 s |
| `Dependency.archive(1000 files)`               |       0.884 s |          0.005 s |
| `Dependency.bit_depth(1000 files)`             |       1.044 s |          0.004 s |
| `Dependency.channels(1000 files)`              |       1.018 s |          0.004 s |
| `Dependency.checksum(1000 files)`              |       0.963 s |          0.004 s |
| `Dependency.duration(1000 files)`              |       1.299 s |          0.004 s | 
| `Dependency.format(1000 files)`                |       1.037 s |          0.004 s |
| `Dependency.removed(1000 files)`               |       1.507 s |          0.004 s |
| `Dependency.sampling_rate(1000 files)`         |       1.116 s |          0.004 s |
| `Dependency.type(1000 files)`                  |       1.271 s |          0.004 s |
| `Dependency.version(1000 files)`               |       0.886 s |          0.004 s |
| `Dependency._add_attachment()`                 |       0.090 s |          0.073 s |
| `Dependency._add_media(1000 files)`            |       0.044 s |          0.068 s |
| `Dependency._add_meta()`                       |       0.112 s |          0.118 s |
| `Dependency._drop()`                           |       0.026 s |          0.209 s |
| `Dependency._remove()`                         |       0.057 s |          0.062 s |
| `Dependency._update_media()`                   |       0.103 s |          0.064 s |
| `Dependency._update_media_version(1000 files)` |       1.043 s |          0.008 s |

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


## audb.Dependencies methods

Benchmarks all methods of `audb.Dependencies`
besides `audb.Dependencies.load()`
and `audb.Dependencies.save()`.
This benchmark provides insights
how to best represent
the dependency table internally.

Results based on commit 4bbcc07,
using `pandas.DataFrame`
to represent the dependency table.

| Method                                         | Execution time |
| ---------------------------------------------- | -------------- |
| `Dependency.__call__()`                        |        0.000 s |
| `Dependency.__contains__()`                    |        0.000 s |
| `Dependency.__get_item__()`                    |        0.000 s |
| `Dependency.__len__()`                         |        0.000 s |
| `Dependency.__str__()`                         |        0.006 s |
| `Dependency.archives`                          |        0.147 s |
| `Dependency.attachments`                       |        0.045 s |
| `Dependency.attachment_ids`                    |        0.045 s |
| `Dependency.files`                             |        0.185 s |
| `Dependency.media`                             |        0.264 s |
| `Dependency.removed_media`                     |        0.250 s |
| `Dependency.table_ids`                         |        0.053 s |
| `Dependency.tables`                            |        0.046 s |
| `Dependency.archive(1000 files)`               |        0.005 s |
| `Dependency.bit_depth(1000 files)`             |        0.004 s |
| `Dependency.channels(1000 files)`              |        0.004 s |
| `Dependency.checksum(1000 files)`              |        0.004 s |
| `Dependency.duration(1000 files)`              |        0.004 s | 
| `Dependency.format(1000 files)`                |        0.004 s |
| `Dependency.removed(1000 files)`               |        0.004 s |
| `Dependency.sampling_rate(1000 files)`         |        0.004 s |
| `Dependency.type(1000 files)`                  |        0.005 s |
| `Dependency.version(1000 files)`               |        0.004 s |
| `Dependency._add_attachment()`                 |        0.061 s |
| `Dependency._add_media(1000 files)`            |        0.050 s |
| `Dependency._add_meta()`                       |        0.124 s |
| `Dependency._drop()`                           |        0.078 s |
| `Dependency._remove()`                         |        0.068 s |
| `Dependency._update_media()`                   |        0.073 s |
| `Dependency._update_media_version(1000 files)` |        0.008 s |

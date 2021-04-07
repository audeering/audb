====
audb
====

|tests| |coverage| |docs| |python-versions| |license|

**audb** manages your annotated media files.

Databases used in machine learning
should be easily exchangeable
and versioned for reproducibility.
**audb** allows both
as it stores all databases in a `common format`_
and manages different versions of a database.
Databases are stored in repositories
on local file systems
or Artifactory_ servers.

You can request resampling or remixing of audio content
and filter the downloaded data,
e.g. if you just want to download the test set.

Have a look at the installation_ and quickstart_ instructions.

.. _common format: https://audeering.github.io/audformat/
.. _Artifactory: https://jfrog.com/artifactory/
.. _installation: https://audeering.github.io/audb/install.html
.. _quickstart: https://audeering.github.io/audb/quickstart.html


.. badges images and links:
.. |tests| image:: https://github.com/audeering/audb/workflows/Test/badge.svg
    :target: https://github.com/audeering/audb/actions?query=workflow%3ATest
    :alt: Test status
.. |coverage| image:: https://codecov.io/gh/audeering/audb/branch/master/graph/badge.svg?token=drrULW8vEG
    :target: https://codecov.io/gh/audeering/audb/
    :alt: code coverage
.. |docs| image:: https://img.shields.io/pypi/v/audb?label=docs
    :target: https://audeering.github.io/audb/
    :alt: audb's documentation
.. |license| image:: https://img.shields.io/badge/license-MIT-green.svg
    :target: https://github.com/audeering/audb/blob/master/LICENSE
    :alt: audb's MIT license
.. |python-versions| image:: https://img.shields.io/pypi/pyversions/audb.svg
    :target: https://pypi.org/project/audb/
    :alt: audbs's supported Python versions

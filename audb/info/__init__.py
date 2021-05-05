r"""Get information from database headers.

.. Specify repository to overwrite local config files
.. jupyter-execute::
    :hide-code:
    :hide-output:

    import audb

    audb.config.REPOSITORIES = [
        audb.Repository(
            name='data-public',
            host='https://audeering.jfrog.io/artifactory',
            backend='artifactory',
        )
    ]

.. Pre-load data without being verbose
.. jupyter-execute::
    :stderr:
    :hide-code:
    :hide-output:

    audb.load(
        'emodb',
        version='1.1.1',
        only_metadata=True,
        verbose=False,
    )

Instead of caching the header (:file:`db.yaml`)
of a database first locally to inspect it,
the functions under :mod:`audb.info`
provide you direct access to this information.

So instead of running:

.. jupyter-execute::

    db = audb.load(
        'emodb',
        version='1.1.1',
        only_metadata=True,
        verbose=False,
    )
    db.tables

You can run:

.. jupyter-execute::

    audb.info.tables(
        'emodb',
        version='1.1.1',
    )

"""
from audb.core.info import (
    author,
    bit_depths,
    channels,
    description,
    duration,
    formats,
    header,
    languages,
    license,
    license_url,
    media,
    meta,
    organization,
    raters,
    sampling_rates,
    schemes,
    source,
    splits,
    tables,
    usage,
)

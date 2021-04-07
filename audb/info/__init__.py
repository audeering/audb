r"""Get information from database headers.

Instead of caching the header (:file:`db.yaml`)
of a database first locally to inspect it,
the functions under :mod:`audb.info`
provide you direct access to this information.

So instead of running:

.. jupyter-execute::
    :stderr:
    :hide-code:
    :hide-output:

    import audb


    audb.load(
        'emodb',
        version='1.1.0',
        only_metadata=True,
        verbose=False,
    )


.. jupyter-execute::

    db = audb.load(
        'emodb',
        version='1.1.0',
        only_metadata=True,
    )
    db.tables

You can run:

.. jupyter-execute::

    audb.info.tables(
        'emodb',
        version='1.1.0',
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

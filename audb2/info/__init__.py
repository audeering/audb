r"""Get information from database headers.

Instead of caching the header (:file:`db.yaml`)
of a database first locally to inspect it,
the functions under :mod:`audb2.info`
provide you direct access to this information.

So instead of running:

.. jupyter-execute::
    :stderr:
    :hide-code:
    :hide-output:

    import audb2


    audb2.load(
        'emodb',
        version='1.0.1',
        only_metadata=True,
        verbose=False,
    )


.. jupyter-execute::

    db = audb2.load(
        'emodb',
        version='1.0.1',
        only_metadata=True,
    )
    db.tables

You can run:

.. jupyter-execute::

    audb2.info.tables(
        'emodb',
        version='1.0.1',
    )

"""
from audb2.core.info import (
    bit_depths,
    channels,
    description,
    duration,
    formats,
    header,
    languages,
    media,
    meta,
    raters,
    sampling_rates,
    schemes,
    source,
    splits,
    tables,
    usage,
)

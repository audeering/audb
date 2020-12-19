r"""Get information from database headers.

Instead of caching the header (:file:`db.yaml`)
of a database first locally to inspect it,
the functions under :mod:`audb2.info`
provide you direct access to this information.

So instead of running:

.. code-block:: python

    db = audb2.load('emodb', version='1.0.1', only_metadata=True)
    db.tables

You can run:

.. code-block:: python

    audb2.info.tables('emodb', version='1.0.1')

"""
from audb2.core.info import (
    description,
    languages,
    media,
    meta,
    raters,
    schemes,
    source,
    splits,
    tables,
    usage,
)

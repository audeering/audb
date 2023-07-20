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
        version='1.4.1',
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
        version='1.4.1',
        only_metadata=True,
        verbose=False,
    )
    db.tables

You can run:

.. jupyter-execute::

    audb.info.tables(
        'emodb',
        version='1.4.1',
    )

"""
from audb.core.info import attachments
from audb.core.info import author
from audb.core.info import bit_depths
from audb.core.info import channels
from audb.core.info import description
from audb.core.info import duration
from audb.core.info import files
from audb.core.info import formats
from audb.core.info import header
from audb.core.info import languages
from audb.core.info import license
from audb.core.info import license_url
from audb.core.info import media
from audb.core.info import meta
from audb.core.info import misc_tables
from audb.core.info import organization
from audb.core.info import raters
from audb.core.info import sampling_rates
from audb.core.info import schemes
from audb.core.info import source
from audb.core.info import splits
from audb.core.info import tables
from audb.core.info import usage

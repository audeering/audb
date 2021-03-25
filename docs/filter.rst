Loading on demand
=================

.. jupyter-execute::
    :hide-code:
    :hide-output:

    import pandas as pd


    def series_to_html(self):
        df = self.to_frame()
        df.columns = ['']
        return df._repr_html_()
    setattr(pd.Series, '_repr_html_', series_to_html)


    def index_to_html(self):
        return self.to_frame(index=False)._repr_html_()
    setattr(pd.Index, '_repr_html_', index_to_html)


It is possible to request only
specific tables or media of a database.

For instance, many databases are organized
into *train*, *dev*, and *test* splits.
Hence, to evaluate the performance of a model,
we don't have to download the full dastabase,
but only the table(s) and media of the *test* set.

Or, if we want the data of a specific speaker,
we can do the following.
First, we download the table with information
about the speakers (here ``db['files']``):

.. jupyter-execute::

    import audb2


    db = audb2.load(
        'emodb',
        version='1.0.1',
        tables=['files'],
        only_metadata=True,
        full_path=False,
    )
    db.tables

Note that we ``only_metadata=True``
since we only need the labels at the moment.
By setting ``full_path=False``
we further ensure that the paths
in the table index are relative
and therefore match the paths on the backend.

.. jupyter-execute::

    speaker = db['files']['speaker'].get()
    speaker

Now, we use the column with speaker IDs
to get a list of media files
that belong to speaker 3.

.. jupyter-execute::

    media = db['files'].files[speaker == 3]
    media

Finally, we load the database again
and use the list to request
only the data of this speaker.
This will also remove
entries of other speakers
from the tables.

.. jupyter-execute::

    db = audb2.load(
        'emodb',
        version='1.0.1',
        media=media,
    )
    db['emotion'].get()

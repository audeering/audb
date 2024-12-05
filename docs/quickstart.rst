.. _quickstart:

Quickstart
==========

The most common task is to load a database
with :func:`audb.load`.

You can browse `available datasets`_
or check them in your Python console:

>>> import audb
>>> audb.available(only_latest=True)
                         backend  ... version
name                              ...
...
emodb                artifactory  ...   1.4.1
...

Let's load version 1.4.1 of the emodb_ database.

.. Load with only_metadata=True in the background
.. invisible-code-block: python

    db = audb.load(
        "emodb",
        version="1.4.1",
        only_metadata=True,
        full_path=False,
        verbose=False,
    )
    # Add flavor path, to mimic `full_path=True`
    flavor_path = audb.flavor_path("emodb", "1.4.1").replace("\\", "/")
    for table in list(db.tables):
        db[table]._df.index = f"...{flavor_path}/" + db[table]._df.index

.. skip: next

>>> db = audb.load("emodb", version="1.4.1", verbose=False)

This downloads the database header,
all the media files,
and tables with annotations
to a caching folder on your machine.
The database is then returned
as an :class:`audformat.Database` object.

Each database comes with a description,
which is a good starting point
to learn what the database is all about.

>>> db.description[:78]
'Berlin Database of Emotional Speech. A German database of emotional utterances'

The annotations of a database are stored in
tables represented by :class:`audformat.Table`.

>>> db.tables
emotion:
  type: filewise
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.test.gold_standard:
  type: filewise
  split_id: test
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
emotion.categories.train.gold_standard:
  type: filewise
  split_id: train
  columns:
    emotion: {scheme_id: emotion, rater_id: gold}
    emotion.confidence: {scheme_id: confidence, rater_id: gold}
files:
  type: filewise
  columns:
    duration: {scheme_id: duration}
    speaker: {scheme_id: speaker}
    transcription: {scheme_id: transcription}

Each table contains columns (:class:`audformat.Column`)
that have corresponding schemes (:class:`audformat.Scheme`)
describing its content.
For example,
to get an idea about the emotion annotations
stored in the ``emotion`` column,
we can inspect the corresponding scheme.

>>> db.schemes["emotion"]
description: Six basic emotions and neutral.
dtype: str
labels: [anger, boredom, disgust, fear, happiness, sadness, neutral]

Finally, we get the actual annotations
as a :class:`pandas.DataFrame`.

>>> df = db["emotion"].get()  # get table
>>> df[:3]  # show first three entries
                                           emotion  emotion.confidence
file
...emodb/1.4.1/d3b62a9b/wav/03a01Fa.wav  happiness                0.90
...emodb/1.4.1/d3b62a9b/wav/03a01Nc.wav    neutral                1.00
...emodb/1.4.1/d3b62a9b/wav/03a01Wa.wav      anger                0.95


.. _emodb: https://github.com/audeering/emodb
.. _available datasets: https://audeering.github.io/datasets/datasets.html

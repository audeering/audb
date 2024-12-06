.. _quickstart:

Quickstart
==========

Browse `available datasets`_ and select one.
We load emodb_,
which is returned
as an :class:`audformat.Database`.

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

>>> db = audb.load("emodb", version="1.4.1")

Inspect label schemes,
and request emotion labels for the test split.

>>> list(db.schemes)
['age',
 'confidence',
 'duration',
 'emotion',
 'gender',
 'language',
 'speaker',
 'transcription']
>>> db.get("emotion", splits="test")  # returns dataframe
                       emotion
file
.../wav/12a01Fb.wav  happiness
.../wav/12a01Lb.wav    boredom
.../wav/12a01Nb.wav    neutral
.../wav/12a01Wc.wav      anger
.../wav/12a02Ac.wav       fear
...                        ...

Or inspect tables,
and request labels from a table.

>>> list(db)
['emotion',
 'emotion.categories.test.gold_standard',
 'emotion.categories.train.gold_standard',
 'files',
 'speaker']
>>> db["emotion.categories.test.gold_standard"].get()  # returns dataframe
                       emotion  emotion.confidence
file
.../wav/12a01Fb.wav  happiness                0.95
.../wav/12a01Lb.wav    boredom                0.90
.../wav/12a01Nb.wav    neutral                0.95
.../wav/12a01Wc.wav      anger                0.95
.../wav/12a02Ac.wav       fear                0.90
...                        ...                 ...

Load a media file,
selected from the index of the dataframe
or from the files index ``db.files``.

.. skip: start

>>> import audiofile
>>> signal, sampling_rate = audiofile.read(db.files[0])

Listen to the signal.

>>> import sounddevice
>>> sounddevice.play(signal.T, sampling_rate)

.. skip: end


.. _emodb: https://github.com/audeering/emodb
.. _available datasets: https://audeering.github.io/datasets/datasets.html

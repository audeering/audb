audb.info
==========

Get information from database headers.

Instead of caching the header (:file:`db.yaml`)
of a database first locally to inspect it,
the functions under :mod:`audb.info`
provide you direct access to this information.

So instead of running:

>>> db = audb.load("emodb", version="1.4.1", only_metadata=True, verbose=False)
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


You can run:

>>> audb.info.tables("emodb", version="1.4.1")
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


.. automodule:: audb.info

.. autosummary::
    :toctree:
    :nosignatures:

    attachments
    author
    bit_depths
    channels
    description
    duration
    files
    formats
    header
    languages
    license
    license_url
    media
    meta
    misc_tables
    organization
    raters
    sampling_rates
    schemes
    source
    splits
    tables
    usage

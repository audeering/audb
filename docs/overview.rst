Overview
========

In the process of providing you the data,
:mod:`audb2`

1. finds the latest version of a database (optional)
2. calculates :ref:`database dependencies <database-dependencies>`
3. downloads archive files from a selected backend (e.g. Artifactory)
4. unpacks the archive files
5. inspects and :ref:`converts <database-conversion-and-flavors>`
   the audio files (optional)
6. stores the data in a :ref:`cache <cache-root>` folder

.. graphviz:: pics/workflow.dot

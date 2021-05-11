Installation
============

To install :mod:`audb` run:

.. code-block:: bash

    $ # Create and activate Python virtual environment, e.g.
    $ # virtualenv --python=python3 ${HOME}/.envs/audb
    $ # source ${HOME}/.envs/audb/bin/activate
    $ pip install audb

:mod:`audb` uses :mod:`audiofile` to access media files,
which supports WAV, FLAC, OGG out of the box.
:mod:`audiofile` requires the libsndfile_ C library,
which should be installed on most systems.
Under Ubuntu you can install it with

.. code-block:: bash

    $ sudo apt-get install libsndfile1

In order to handle all possible audio files,
please make sure ffmpeg_,
sox_,
and mediainfo_
are installed on your system.
Under Ubuntu this can be achieved with

.. code-block:: bash

    $ sudo apt-get install ffmpeg mediainfo sox libsox-fmt-mp3

Under Windows you have to install those libraries manually,
and ensure that they are added to the ``PATH`` variable.


.. _libsndfile: https://github.com/libsndfile/libsndfile
.. _ffmpeg: https://www.ffmpeg.org/
.. _sox: http://sox.sourceforge.net/
.. _mediainfo: https://mediaarea.net/en/MediaInfo/

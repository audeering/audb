.. raw:: html

    <p align="center">
      <img src="logo.png">
    </p>

    <div align="center">

|docs| |python-versions| |license|


.. raw:: html

    </div>

**audb** manages your annotated media files.

Databases used in machine learning
should be easily exchangeable
and versioned for reproducibility.
**audb** allows both
as it stores all databases in a `common format`_
and manages different versions of a database.
Databases are stored in repositories
on local file systems,
MinIO_,
S3_,
or Artifactory_ servers.

You can request resampling or remixing of audio content
and filter the downloaded data,
e.g. if you just want to download the test set.

Have a look at the installation_ and quickstart_ instructions.


Citation
--------

If you want to cite **audb**, you can refer to our paper_:

.. code-block:: bibtex

    @article{wierstorf2023audb,
      title={audb -- Sharing and Versioning of Audio and Annotation Data in Python},
      author={Hagen Wierstorf
        and Johannes Wagner
        and Florian Eyben
        and Felix Burkhardt
        and Bj{\"o}rn W. Schuller},
      journal={arXiv preprint arXiv:2303.00645},
      year={2023},
    }


.. _Artifactory: https://jfrog.com/artifactory/
.. _common format: https://audeering.github.io/audformat/
.. _installation: https://audeering.github.io/audb/install.html
.. _MinIO: https://min.io
.. _paper: https://arxiv.org/abs/2303.00645
.. _quickstart: https://audeering.github.io/audb/quickstart.html
.. _S3: https://aws.amazon.com/s3/


.. header links:
.. |docs| replace:: `Documentation <https://audeering.github.io/audb/>`__
.. |license| replace:: `MIT license <https://github.com/audeering/audb/blob/main/LICENSE>`__
.. |python-versions| replace:: `Python 3.9, 3.10, 3.11, 3.12, 3.13 <https://pypi.org/project/audb/>`__

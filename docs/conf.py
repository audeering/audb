import configparser
from datetime import date
import os
import subprocess


config = configparser.ConfigParser()
config.read(os.path.join('..', 'setup.cfg'))

# Project -----------------------------------------------------------------
author = config['metadata']['author']
copyright = f'2020-{date.today().year} audEERING GmbH'
project = config['metadata']['name']
# The x.y.z version read from tags
try:
    version = subprocess.check_output(
        ['git', 'describe', '--tags', '--always']
    )
    version = version.decode().strip()
except Exception:
    version = '<unknown>'
title = f'{project} Documentation'


# General -----------------------------------------------------------------
master_doc = 'index'
extensions = []
source_suffix = '.rst'
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '**.ipynb_checkpoints']
pygments_style = None
extensions = [
    'sphinx.ext.graphviz',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # support for Google-style docstrings
    'sphinx_autodoc_typehints',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx_copybutton',
    'sphinxcontrib.katex',  # has to be before jupyter_sphinx
    'jupyter_sphinx',
]

napoleon_use_ivar = True  # List of class attributes
autodoc_inherit_docstrings = False  # disable docstring inheritance
intersphinx_mapping = {
    'audeer': ('https://audeering.github.io/audeer/', None),
    'audformat': ('http://audeering.github.io/audformat/', None),
    'audiofile': ('https://audeering.github.io/audiofile/', None),
    'audobject': ('http://tools.pp.audeering.com/audobject/', None),
    'audresample': ('http://tools.pp.audeering.com/pyaudresample//', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'python': ('https://docs.python.org/3/', None),
}
linkcheck_ignore = [
    'https://gitlab.audeering.com',
]
# Ignore package dependencies during building the docs
# This fixes URL link issues with pandas and sphinx_autodoc_typehints
autodoc_mock_imports = [
    'pandas',
]
graphviz_output_format = 'svg'

# HTML --------------------------------------------------------------------
html_theme = 'sphinx_audeering_theme'
html_theme_options = {
    'display_version': True,
    'logo_only': False,
    'wide_pages': ['data-example'],
}
html_title = title

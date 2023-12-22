from datetime import date
import os
import shutil

import toml

import audeer

import audb


config = toml.load(audeer.path('..', 'pyproject.toml'))


# Project -----------------------------------------------------------------
project = config['project']['name']
copyright = f'2020-{date.today().year} audEERING GmbH'
author = ', '.join(author['name'] for author in config['project']['authors'])
version = audeer.git_repo_version()
title = project


# General -----------------------------------------------------------------
master_doc = 'index'
source_suffix = '.rst'
exclude_patterns = [
    'api-src',
    'build',
    'tests',
    'Thumbs.db',
    '.DS_Store',
]
templates_path = ['_templates']
pygments_style = None
extensions = [
    'sphinx.ext.graphviz',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # support for Google-style docstrings
    'sphinx.ext.autosummary',
    'sphinx_autodoc_typehints',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx_copybutton',
    'jupyter_sphinx',
]

napoleon_use_ivar = True  # List of class attributes
# autodoc_inherit_docstrings = False  # disable docstring inheritance
intersphinx_mapping = {
    'audbackend': ('https://audeering.github.io/audbackend/', None),
    'audeer': ('https://audeering.github.io/audeer/', None),
    'audformat': ('http://audeering.github.io/audformat/', None),
    'audiofile': ('https://audeering.github.io/audiofile/', None),
    'audobject': ('https://audeering.github.io/audobject/', None),
    'audresample': ('https://audeering.github.io/audresample/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'python': ('https://docs.python.org/3/', None),
}
linkcheck_ignore = [
    'https://gitlab.audeering.com',
    (  # This is a permalink, which provides a false-positive result
        'https://github.com/audeering/audbackend/blob/'
        'edd23462799ae9052a43cdd045698f78e19dbcaf'
    ),
    'http://emodb.bilderbar.info/start.html',
    # This permalink fails from time to time and should never be broken
    'https://superuser.com/a/264406',
]
# Ignore package dependencies during building the docs
# This fixes URL link issues with pandas and sphinx_autodoc_typehints
autodoc_mock_imports = [
    'pandas',
]
graphviz_output_format = 'svg'

# Disable auto-generation of TOC entries in the API
# https://github.com/sphinx-doc/sphinx/issues/6316
toc_object_entries = False

# HTML --------------------------------------------------------------------
html_theme = 'furo'
html_static_path = ['_static']
html_theme_options = {
    'source_repository': 'https://github.com/audeering/audb',
    'source_branch': 'main',
    'source_directory': 'docs/',
    # Logo
    'light_logo': 'logo-black.png',
    'dark_logo': 'logo-white.png',
    # Colors
    'light_css_variables': {
        'color-brand-primary': '#e13b41',
        'color-brand-content': '#3c4350',
        'color-code-foreground': '#3c4350',
        'color-code-background': '#f7f7fd',
        'color-content-foreground': '#3c4350',
        'color-sidebar-background': '#ffffff',
        'color-sidebar-search-background': '#ffffff',
        'color-api-pre-name': '#e13b41',
        'color-api-name': '#e13b41',
        'color-api-overall': '#5d6370',
        'color-api-background-hover': '#f7f7fd',
        'color-link': '#e13b41',
        'color-link--hover': '#e13b41',
        'color-link-underline': '#d6d7e5',
        'color-link-underline--hover': '#e13b41',
        'color-inline-code-background': '#f7f7fd',
        'color-admonition-title--caution': '#f0aa3a',
        'color-admonition-title-background--caution': 'rgba(240,170,58,.2)',
        'color-admonition-title--warning': 'f0aa3a',
        'color-admonition-title-background--warning': 'rgba(240,170,58,.2)',
        'color-admonition-title--danger': '#e94274',
        'color-admonition-title-background--danger': 'rgba(233,66,116,.2)',
        'color-admonition-title--attention': '#e94274',
        'color-admonition-title-background--attention': 'rgba(233,66,116,.2)',
        'color-admonition-title--error': '#e94274',
        'color-admonition-title-background--error': 'rgba(233,66,116,.2)',
        'color-admonition-title--hint': '#35e17a',
        'color-admonition-title-background--hint': 'rgba(53,255,122,.2)',
        'color-admonition-title--tip': '#35e17a',
        'color-admonition-title-background--tip': 'rgba(53,255,122,.2)',
        'color-admonition-title--important': '#f0aa3a',
        'color-admonition-title-background--important': 'rgba(240,170,58,.2)',
        'color-admonition-title--note': '#32adf2',
        'color-admonition-title-background--note': 'rgba(50,173,242,.2)',
        'color-admonition-title--seealso': '#32adf2',
        'color-admonition-title-background--seealso': 'rgba(50,173,242,.2)',
        'color-admonition-title--todo': '#32adf2',
        'color-admonition-title-background--todo': 'rgba(50,173,242,.2)',
        # 'color-admonition-title': '',
        # 'color-admonition-title-background': '',
        # TODO:
        # * color of code listings has to be set in CSS
        #   by using:
        #   .highlight {
        #     background: #f7f7d;
        #   }
        # * adjust size of headings:
        #   h1 {
        #     font-size: 2.25em;
        #   }
        #   h2 {
        #     font-size: 1.75em;
        #   }
        #
        # * disable border of inline code,
        #   and adjust padding for missing border:
        #   p .sig-inline, p code.literal {
        #     border: none;
        #   }
        #   .sig-inline, code.literal {
        #     padding: 0.15em 0.25em;
        #   }
        #
        # * center sidebar title
        #   .sidebar-brand-text {
        #     text-align: center;
        #   }
        'font-stack': 'Open Sans, sans-serif',
    }
}
html_context = {
    'display_github': True,
}
html_title = title


# Cache databases to avoid progress bar in code examples ------------------
audb.config.REPOSITORIES = [
    audb.Repository(
        name='data-public',
        host='https://audeering.jfrog.io/artifactory',
        backend='artifactory',
    )
]
database_name = 'emodb'
database_version = '1.4.1'
if not audb.exists(database_name, version=database_version):
    print(f'Pre-caching {database_name} v{database_version}')
    audb.load(
        database_name,
        version=database_version,
        num_workers=5,
        only_metadata=True,
        verbose=False,
    )
if not audb.exists(
        database_name,
        version=database_version,
        format='flac',
        sampling_rate=44100,
):
    print(f'Pre-caching {database_name} v{database_version} {{flac, 44100Hz}}')
    audb.load(
        database_name,
        version=database_version,
        format='flac',
        sampling_rate=44100,
        num_workers=5,
        only_metadata=True,
        verbose=False,
    )


# Copy API (sub-)module RST files to docs/api/ folder ---------------------
audeer.rmdir('api')
audeer.mkdir('api')
api_src_files = audeer.list_file_names('api-src')
api_dst_files = [
    audeer.path('api', os.path.basename(src_file))
    for src_file in api_src_files
]
for src_file, dst_file in zip(api_src_files, api_dst_files):
    shutil.copyfile(src_file, dst_file)

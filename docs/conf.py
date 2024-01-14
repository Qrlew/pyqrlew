# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Pyqrlew'
copyright = '2023, Sarus Technologies'
author = 'Sarus Technologies'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # 'myst_parser',
    'myst_nb',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinxcontrib.bibtex',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Enable numref
numfig = True
bibtex_bibfiles = ['qrlew.bib']

# MyST conf
myst_enable_extensions = [
    'dollarmath',
    'amsmath',
]
myst_dmath_allow_labels = True
myst_dmath_double_inline = True

# Controls notebook execution
nb_execution_mode = 'cache'#'off'/'cache'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'

html_static_path = ['_static']

html_theme_options = {
    'navbar_start': ['navbar-logo'],
    'navbar_center': ['navbar-nav'],
    'navbar_end': ['navbar-icon-links'],
    'navbar_persistent': ['search-button'],
    'icon_links': [
        {
            'name': 'GitHub',
            'url': 'https://github.com/Qrlew',
            'icon': 'fa-brands fa-github',
            'type': 'fontawesome',
        },
        {
            'name': 'Web Page',
            'url': 'https://qrlew.github.io/',
            'icon': 'fa-solid fa-house',
            'type': 'fontawesome',
        },
        {
            'name': 'Discord',
            'url': 'https://discord.gg/JbvSPgyp',
            'icon': 'fa-brands fa-discord',
            'type': 'fontawesome',
        },
        {
            'name': 'X',
            'url': 'https://x.com/sarus_tech',
            'icon': 'fa-brands fa-twitter',
            # The default for `type` is `fontawesome` so it is not actually required in any of the above examples as it is shown here
        },
    ],
}
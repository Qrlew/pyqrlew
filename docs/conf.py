# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

import inspect
import os
import sys
import warnings
from pathlib import Path
from typing import Any

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Pyqrlew'
copyright = '2023, Sarus Technologies'
author = 'Sarus Technologies'
github_root = "https://github.com/Qrlew/pyqrlew"
git_branch = 'main'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # 'myst_parser',
    'myst_nb',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon', # it understands google and numpy style doc strings
    'sphinx.ext.intersphinx', # it add links to objects
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinxcontrib.bibtex',
    "sphinx.ext.linkcode",
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Extension settings  -----------------------------------------------------

# sphinx.ext.intersphinx - link to other projects' documentation
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Sphinx-copybutton - add copy button to code blocks
# https://sphinx-copybutton.readthedocs.io/en/latest/index.html
# strip the '>>>' and '...' prompt/continuation prefixes.
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

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


autodoc_typehints = 'both' # type hints both in the signature and in the docstring 
# autodoc_typehints_description_target = 'documented'
autodoc_docstring_signature = False
autodoc_member_order = 'bysource' # show members by their source order (it doesn't seem to have an effect)
# Controls notebook execution
nb_execution_mode = 'off'#'off'/'cache'


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
            'url': 'https://discord.com/invite/XQ9jYshj2p',
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

import pyqrlew

# sphinx-ext-linkcode - Add external links to source code
# https://www.sphinx-doc.org/en/master/usage/extensions/linkcode.html
def linkcode_resolve(domain: str, info: dict[str, Any]) -> str | None:
    """
    Determine the URL corresponding to Python object.

    Based on pandas equivalent:
    https://github.com/pandas-dev/pandas/blob/main/doc/source/conf.py#L629-L686
    """
    if domain != "py":
        return None

    modname = info["module"]
    fullname = info["fullname"]

    submod = sys.modules.get(modname)
    if submod is None:
        return None

    obj = submod
    for part in fullname.split("."):
        try:
            with warnings.catch_warnings():
                # Accessing deprecated objects will generate noisy warnings
                warnings.simplefilter("ignore", FutureWarning)
                obj = getattr(obj, part)
        except AttributeError:
            return None

    try:
        fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError:
        try:  # property
            fn = inspect.getsourcefile(inspect.unwrap(obj.fget))
        except (AttributeError, TypeError):
            fn = None
    if not fn:
        return None

    try:
        source, lineno = inspect.getsourcelines(obj)
    except TypeError:
        try:  # property
            source, lineno = inspect.getsourcelines(obj.fget)
        except (AttributeError, TypeError):
            lineno = None
    except OSError:
        lineno = None

    linespec = f"#L{lineno}-L{lineno + len(source) - 1}" if lineno else ""

    # https://github.com/Qrlew/pyqrlew/blob/main/pyqrlew/python/pyqrlew/io/postgresql.py#L16-L167
    # https://github.com/Qrlew/pyqrlew/blob/main/python/pyqrlew/io/postgresql.py
    # fn is the absolute path of a specific source in the environment where pyqrlew is installed
    # to get the relative path I could use pyqrlew.__file__ as follow 
    fn = Path("python") / Path("pyqrlew") /  os.path.relpath(fn, start=os.path.dirname(pyqrlew.__file__))
    return f"{github_root}/blob/{git_branch}/{fn}{linespec}"

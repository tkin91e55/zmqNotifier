# pylint: disable=W0621
"""Sphinx configuration for agentic_py documentation."""

from __future__ import annotations

import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

PROJECT = "agentic_py"
AUTHOR = "agentic_py maintainers"
COPYRIGHT = f"{datetime.now():%Y}, {AUTHOR}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

autodoc_mock_imports: list[str] = []

templates_path = ["_templates"]
exclude_patterns: list[str] = []

master_doc = "index"

HTML_THEME = "alabaster"
html_static_path = ["_static"]

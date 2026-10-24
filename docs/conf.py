"""Build the maintained Markdown with Mooncake's Sphinx Book Theme."""

import os

project = "OrbitTE"
author = "OrbitInfra"
copyright = "2026, OrbitInfra"
html_title = project
html_theme = "sphinx_book_theme"
html_logo = "_static/orbitte-logo.svg"
html_favicon = "_static/orbit-icon.svg"
html_static_path = ["_static"]
html_extra_path = ["_extra"]
html_css_files = ["orbit.css"]
html_show_sourcelink = False

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]
source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = ["requirements.*"]
myst_enable_extensions = ["colon_fence", "fieldlist"]
myst_fence_as_directive = ["mermaid"]
myst_heading_anchors = 6
nitpicky = True
copybutton_prompt_text = r"\$ "
copybutton_prompt_is_regexp = True

html_theme_options = {
    "repository_url": "https://github.com/OrbitInfra/OrbitTE",
    "repository_branch": os.environ.get("ORBITTE_SOURCE_REF", "main"),
    "path_to_docs": "docs",
    "use_repository_button": True,
    "use_edit_page_button": True,
    "collapse_navbar": True,
    "show_navbar_depth": 2,
    "show_toc_level": 2,
    "logo": {
        "image_light": "_static/orbitte-logo.svg",
        "image_dark": "_static/orbitte-logo-dark.svg",
    },
}

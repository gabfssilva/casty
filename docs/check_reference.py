"""Check that the reference documents every name `casty` exports, each with its docstring.

`mkdocs build --strict` fails on a `::: path` it cannot resolve, and renders a name without a docstring in silence.
This checks what the build cannot: that each name of `casty.__all__` has a docstring, and that some page of
`docs/reference` renders it. The names are read from the sources as the build reads them, without importing the
compiled extension.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import griffe

ROOT = Path(__file__).resolve().parent.parent


def problems() -> list[str]:
    """What is wrong with the reference, one line per problem."""
    overloads = griffe.load_extensions(str(ROOT / "docs" / "griffe_overloads.py"))
    casty = griffe.load("casty", search_paths=[ROOT / "src"], allow_inspection=False, extensions=overloads)
    if casty.exports is None:
        return ["casty declares no __all__"]
    pages = (page.read_text(encoding="utf-8") for page in (ROOT / "docs" / "reference").glob("*.md"))
    rendered = {path for page in pages for path in re.findall(r"^::: (\S+)\s*$", page, re.MULTILINE)}
    found: list[str] = []
    for name in map(str, casty.exports):
        target = casty[name]
        if isinstance(target, griffe.Alias):
            target = target.final_target
        if target.docstring is None or not target.docstring.value.strip():
            found.append(f"casty.{name} has no docstring")
        paths = sorted({f"casty.{name}", target.path})
        if rendered.isdisjoint(paths):
            directives = " or ".join(f"`::: {path}`" for path in paths)
            found.append(f"casty.{name} is not in the reference: no page has {directives}")
    return found


def main() -> int:
    found = problems()
    for problem in found:
        print(problem, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

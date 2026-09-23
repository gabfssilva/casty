"""Check that the built reference documents every name `casty` exports, each with its docstring.

Run after `mkdocs build`, with the directory it wrote (`site` by default). The names are read from the sources as the
build reads them, without importing the compiled extension. Each one must have a docstring, and some page must have a
heading anchored at the object it names, followed by the first paragraph of that docstring.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path

import griffe

ROOT = Path(__file__).resolve().parent.parent


@dataclass(slots=True)
class _Reading:
    """An object whose text is being read: the depth of its `doc-contents`, and of the members it is skipping."""

    anchor: str
    depth: int
    skipping: int | None = None
    text: list[str] = field(default_factory=list[str])


class Sections(HTMLParser):
    """The text of each object on a page, by the id of its heading.

    mkdocstrings writes an object as its heading, then a `doc-contents` block with its docstring and, nested in a
    `doc-children` block, its members. The text of an object leaves its members out.
    """

    def __init__(self) -> None:
        super().__init__()
        self.found: dict[str, str] = {}
        self._heading: str | None = None
        self._depth = 0
        self._reading: list[_Reading] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        named = dict(attrs)
        classes = (named.get("class") or "").split()
        anchor = named.get("id")
        if "doc-heading" in classes and anchor:
            self._heading = anchor
        if tag != "div":
            return
        self._depth += 1
        if "doc-contents" in classes and self._heading is not None:
            self._reading.append(_Reading(self._heading, self._depth))
            self._heading = None
        elif "doc-children" in classes:
            for reading in self._reading:
                if reading.skipping is None:
                    reading.skipping = self._depth

    def handle_endtag(self, tag: str) -> None:
        if tag != "div":
            return
        for reading in self._reading:
            if reading.skipping == self._depth:
                reading.skipping = None
            if reading.depth == self._depth:
                self.found[reading.anchor] = " ".join("".join(reading.text).split())
        self._reading = [reading for reading in self._reading if reading.depth != self._depth]
        self._depth -= 1

    def handle_data(self, data: str) -> None:
        for reading in self._reading:
            if reading.skipping is None:
                reading.text.append(data)


def rendered(site: Path) -> dict[str, str]:
    """The text of every object documented under `site`, by the id of its heading."""
    found: dict[str, str] = {}
    for page in site.rglob("*.html"):
        sections = Sections()
        sections.feed(page.read_text(encoding="utf-8"))
        found.update(sections.found)
    return found


def summary(docstring: str) -> str:
    """The first paragraph of `docstring` as a page shows it: without the backticks of inline code."""
    return " ".join(docstring.split("\n\n", 1)[0].replace("`", "").split())


def problems(site: Path) -> list[str]:
    """What is wrong with the reference under `site`, one line per exported name."""
    overloads = griffe.load_extensions(str(ROOT / "docs" / "griffe_overloads.py"))
    casty = griffe.load("casty", search_paths=[ROOT / "src"], allow_inspection=False, extensions=overloads)
    if casty.exports is None:
        return ["casty declares no __all__"]
    sections = rendered(site)
    found: list[str] = []
    for name in map(str, casty.exports):
        target = casty[name]
        if isinstance(target, griffe.Alias):
            target = target.final_target
        if target.docstring is None or not target.docstring.value.strip():
            found.append(f"casty.{name} has no docstring")
            continue
        text = sections.get(target.path) or sections.get(f"casty.{name}")
        if text is None:
            found.append(f"casty.{name} is not in the reference: no heading is anchored at {target.path}")
        elif summary(target.docstring.value) not in text:
            found.append(f"casty.{name} is in the reference without its docstring")
    return found


def main() -> int:
    site = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "site"
    if not site.is_dir():
        print(f"{site} is not a directory: build the reference first", file=sys.stderr)
        return 2
    found = problems(site)
    for problem in found:
        print(problem, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

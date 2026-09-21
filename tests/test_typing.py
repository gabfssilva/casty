import re
import subprocess
import sys
from pathlib import Path
from typing import Literal, assert_never

import pytest

type Checker = Literal["pyright", "mypy"]

ROOT = Path(__file__).parent.parent
TYPING = Path("tests/typing")


def describe_public_api_types() -> None:
    @pytest.mark.parametrize("checker", ["pyright", "mypy"])
    def it_accepts_the_usage_examples(checker: Checker) -> None:
        assert errors(checker, TYPING / "usage.py") == []

    @pytest.mark.parametrize("checker", ["pyright", "mypy"])
    def it_reports_exactly_the_marked_lines(checker: Checker) -> None:
        path = TYPING / "errors.py"

        assert {line for line, _ in errors(checker, path)} == marked_lines(path, checker)


def errors(checker: Checker, path: Path) -> list[tuple[int, str]]:
    """Errors that `checker` reports in `path`, as line number and message."""
    match checker:
        case "pyright":
            command = ["pyright", "--project", str(TYPING), "--pythonpath", sys.executable, str(path)]
            line = re.compile(rf"^\s*{re.escape(str(ROOT / path))}:(\d+):\d+ - error: (.*)$")
        case "mypy":
            command = ["mypy", "--no-error-summary", str(path)]
            line = re.compile(rf"^{re.escape(str(path))}:(\d+): error: (.*)$")
        case _:
            assert_never(checker)
    output = subprocess.run([sys.executable, "-m", *command], cwd=ROOT, capture_output=True, text=True).stdout
    return [(int(found[1]), found[2]) for found in map(line.match, output.splitlines()) if found]


def marked_lines(path: Path, checker: Checker) -> set[int]:
    """Lines of `path` ending in `# error`, or in `# error: <checker>`."""
    lines = (ROOT / path).read_text().splitlines()
    return {number for number, text in enumerate(lines, start=1) if re.search(rf"# error(: {checker})?$", text)}

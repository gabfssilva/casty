import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
TYPING = Path("tests/typing")


def describe_public_api_types() -> None:
    def it_accepts_the_usage_examples() -> None:
        assert errors(TYPING / "usage.py") == []

    def it_reports_exactly_the_marked_lines() -> None:
        path = TYPING / "errors.py"

        assert {line for line, _ in errors(path)} == marked_lines(path)

    def it_declares_the_api_in_the_grammar_of_python_3_12() -> None:
        # pyright accepts newer syntax in a stub whatever its `pythonVersion`; the parser of 3.12, which mypy uses
        # there, does not.
        ast.parse((ROOT / "src/casty/__init__.pyi").read_text(), feature_version=(3, 12))


def errors(path: Path) -> list[tuple[int, str]]:
    """Errors that pyright reports in `path`, as line number and message."""
    command = ["pyright", "--project", str(TYPING), "--pythonpath", sys.executable, str(path)]
    line = re.compile(rf"^\s*{re.escape(str(ROOT / path))}:(\d+):\d+ - error: (.*)$")
    output = subprocess.run([sys.executable, "-m", *command], cwd=ROOT, capture_output=True, text=True).stdout
    return [(int(found[1]), found[2]) for found in map(line.match, output.splitlines()) if found]


def marked_lines(path: Path) -> set[int]:
    """Lines of `path` ending in `# error`."""
    lines = (ROOT / path).read_text().splitlines()
    return {number for number, text in enumerate(lines, start=1) if text.endswith("# error")}

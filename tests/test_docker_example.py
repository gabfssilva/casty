import runpy
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from casty import Unavailable


@pytest.mark.parametrize("error", [TimeoutError, Unavailable])
def test_client_continues_after_rounds_without_responses(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], error: type[Exception]
) -> None:
    example = Path(__file__).resolve().parents[1] / "examples" / "09-docker-cluster"
    monkeypatch.setenv("SEEDS", "localhost:7400")
    monkeypatch.setenv("ROUNDS", "2")
    with (
        patch.object(sys, "path", [str(example), *sys.path]),
        patch("casty.Client") as client,
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        connection = MagicMock()
        connection.ref.return_value.ask = AsyncMock(side_effect=error)
        client.return_value.__aenter__.return_value = connection
        runpy.run_path(str(example / "client.py"), run_name="__main__")
    output = capsys.readouterr().out
    assert "round 1: 0/500 pages answered" in output
    assert "round 2: 0/500 pages answered" in output

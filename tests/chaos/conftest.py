"""The chaos run starts only when asked for with `CASTY_CHAOS=1`: a plain `pytest` does not even collect it.

The tests of its planner and of its checks start nothing, and run with the rest.
"""

from __future__ import annotations

import os

collect_ignore: list[str] = [] if os.environ.get("CASTY_CHAOS") == "1" else ["test_chaos.py"]

import os
import subprocess
import sys
import sysconfig

import pytest


@pytest.mark.skipif(not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="the interpreter has a GIL")
def test_importing_casty_leaves_the_gil_disabled() -> None:
    # An extension that does not declare itself safe without the GIL turns it back on at import, with only a warning.
    env = {name: value for name, value in os.environ.items() if name != "PYTHON_GIL"}
    probe = "import sys, casty; sys.exit(sys._is_gil_enabled())"
    subprocess.run([sys.executable, "-c", probe], env=env, check=True)

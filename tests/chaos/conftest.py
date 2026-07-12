import os

# The chaos suite only runs inside the docker driver (tests/chaos/run.sh),
# where the cluster, the docker socket, and the docker SDK exist.
collect_ignore_glob = ["test_*.py"] if os.environ.get("CASTY_CHAOS") != "1" else []

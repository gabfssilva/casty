#!/bin/bash
# Destroy loadtest infrastructure
# Usage: ./scripts/teardown.sh
set -euo pipefail
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-""}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")/infra"

echo "=== Tearing Down Load Test Infrastructure ==="
cd "$INFRA_DIR"
pulumi destroy --yes --stack dev

echo ""
echo "=== Infrastructure Destroyed ==="

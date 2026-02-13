#!/bin/bash
# Deploy loadtest infrastructure and copy test suite to load generator
# Usage: ./scripts/deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOADTEST_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$LOADTEST_DIR/infra"

echo "=== Deploying Load Test Infrastructure ==="

# Install infra dependencies
echo "  Installing Pulumi dependencies..."
uv sync --project "$LOADTEST_DIR" --extra infra --quiet

# Deploy infra
cd "$INFRA_DIR"
if ! pulumi stack ls --json 2>/dev/null | grep -q '"name": "dev"'; then
    pulumi stack init dev
fi
pulumi up --yes --stack dev

# Get outputs
LOADGEN_IP=$(pulumi stack output loadgen_public_ip --stack dev)
ALB_URL=$(pulumi stack output alb_url --stack dev)

echo ""
echo "=== Infrastructure Ready ==="
echo "Load generator: $LOADGEN_IP"
echo "ALB URL: $ALB_URL"

# Copy loadtest files to load generator
echo ""
echo "=== Copying loadtest to load generator ==="
# Auto-detect SSH key
if [[ -n "${SSH_KEY:-}" ]]; then
    :
elif [[ -f "$HOME/.ssh/id_ed25519" ]]; then
    SSH_KEY="$HOME/.ssh/id_ed25519"
elif [[ -f "$HOME/.ssh/id_rsa" ]]; then
    SSH_KEY="$HOME/.ssh/id_rsa"
elif [[ -f "$HOME/.ssh/id_ecdsa" ]]; then
    SSH_KEY="$HOME/.ssh/id_ecdsa"
else
    echo "Error: No SSH key found in ~/.ssh/" >&2
    exit 1
fi
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-""}"

scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -r \
    "$LOADTEST_DIR"/*.py \
    "$LOADTEST_DIR"/metrics.py \
    "$LOADTEST_DIR"/cluster.py \
    "$LOADTEST_DIR"/load.py \
    "$LOADTEST_DIR"/faults.py \
    "$LOADTEST_DIR"/report.py \
    "$LOADTEST_DIR"/timeline.py \
    "$LOADTEST_DIR"/config.py \
    ec2-user@"$LOADGEN_IP":~/loadtest/

echo ""
echo "=== Ready to Run ==="
echo "SSH into load generator:"
echo "  ssh -i $SSH_KEY ec2-user@$LOADGEN_IP"
echo ""
echo "Run the load test:"
echo "  python3.13 -m loadtest --target-url $ALB_URL --ssh-key ~/.ssh/casty.pem"
echo ""
echo "Quick smoke test (no faults):"
echo "  python3.13 -m loadtest --target-url $ALB_URL --num-workers 10 --duration 60 --no-faults"

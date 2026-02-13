#!/bin/bash
# One command to rule them all: deploy → test → report → teardown
#
# Usage:
#   ./scripts/run.sh                          # full suite (load + faults)
#   ./scripts/run.sh --no-faults              # load only
#   ./scripts/run.sh --duration 120           # custom duration
#   ./scripts/run.sh --keep                   # don't destroy infra after
#
# Requires:
#   - AWS CLI configured
#   - Pulumi installed
#   - Docker running
#   - SSH_KEY env var or ~/.ssh/casty.pem
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOADTEST_DIR="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$LOADTEST_DIR/infra"
# Auto-detect SSH key (same order as Pulumi infra)
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
echo "Using SSH key: $SSH_KEY"
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-""}"
SSH_USER="ec2-user"
KEEP_INFRA=false
LOADTEST_ARGS=()

# Parse our flags vs loadtest flags
for arg in "$@"; do
    if [[ "$arg" == "--keep" ]]; then
        KEEP_INFRA=true
    else
        LOADTEST_ARGS+=("$arg")
    fi
done

cleanup() {
    if [[ "$KEEP_INFRA" == "false" ]]; then
        echo ""
        echo "=== Tearing Down Infrastructure ==="
        cd "$INFRA_DIR"
        if pulumi stack ls --json 2>/dev/null | grep -q '"name": "dev"'; then
            pulumi destroy --yes --stack dev || true
        fi
    else
        echo ""
        echo "=== Infrastructure kept alive (--keep) ==="
        echo "Destroy later with: ./scripts/teardown.sh"
    fi
}
trap cleanup EXIT

echo "=== Casty Load + Resilience Test — Make It Happen ==="
echo ""

# --- 1. Deploy infrastructure ---
echo "=== Step 1/5: Deploying infrastructure ==="

# Install infra dependencies
echo "  Installing Pulumi dependencies..."
uv sync --project "$LOADTEST_DIR" --extra infra --quiet

cd "$INFRA_DIR"
# Init stack (ignore if already exists)
if ! pulumi stack ls --json 2>/dev/null | grep -q '"name": "dev"'; then
    pulumi stack init dev
fi
pulumi up --yes --stack dev

LOADGEN_IP=$(pulumi stack output loadgen_public_ip --stack dev)
ALB_URL=$(pulumi stack output alb_url --stack dev)

echo "Load generator: $LOADGEN_IP"
echo "ALB URL: $ALB_URL"

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

# --- 2. Wait for load generator to be reachable ---
echo ""
echo "=== Step 2/5: Waiting for load generator SSH ==="
for i in $(seq 1 30); do
    if ssh $SSH_OPTS $SSH_USER@"$LOADGEN_IP" "echo ok" &>/dev/null; then
        echo "  Load generator reachable"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  ERROR: Load generator not reachable after 5 minutes"
        exit 1
    fi
    echo "  Waiting... ($i/30)"
    sleep 10
done

# --- 3. Copy loadtest + SSH key to load generator ---
echo ""
echo "=== Step 3/5: Copying loadtest suite + SSH key ==="
ssh $SSH_OPTS $SSH_USER@"$LOADGEN_IP" "mkdir -p ~/loadtest ~/.ssh && chmod 700 ~/.ssh"
scp $SSH_OPTS -r \
    "$LOADTEST_DIR"/__init__.py \
    "$LOADTEST_DIR"/__main__.py \
    "$LOADTEST_DIR"/config.py \
    "$LOADTEST_DIR"/metrics.py \
    "$LOADTEST_DIR"/cluster.py \
    "$LOADTEST_DIR"/load.py \
    "$LOADTEST_DIR"/faults.py \
    "$LOADTEST_DIR"/report.py \
    "$LOADTEST_DIR"/timeline.py \
    $SSH_USER@"$LOADGEN_IP":~/loadtest/

# Copy SSH key for fault injection into cluster nodes
REMOTE_KEY="~/.ssh/loadtest_key"
scp $SSH_OPTS "$SSH_KEY" "$SSH_USER@$LOADGEN_IP:$REMOTE_KEY"
ssh $SSH_OPTS $SSH_USER@"$LOADGEN_IP" "chmod 600 $REMOTE_KEY"

# --- 4. Wait for cluster to be healthy, then run ---
echo ""
echo "=== Step 4/5: Waiting for cluster + running load test ==="

# Wait for uv to be available (installed via user-data)
echo "  Waiting for uv..."
for i in $(seq 1 30); do
    if ssh $SSH_OPTS $SSH_USER@"$LOADGEN_IP" "source ~/.local/bin/env 2>/dev/null; which uv" &>/dev/null; then
        echo "  uv ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "  ERROR: uv not available after 5 minutes"
        exit 1
    fi
    sleep 10
done

ssh $SSH_OPTS -t $SSH_USER@"$LOADGEN_IP" "
set -euo pipefail
source ~/.local/bin/env 2>/dev/null || true

# Wait for ALB to return healthy
echo 'Waiting for cluster to be healthy via $ALB_URL ...'
for i in \$(seq 1 60); do
    if python3.13 -c \"
import urllib.request, json, sys
try:
    r = urllib.request.urlopen('$ALB_URL/cluster/status', timeout=5)
    data = json.loads(r.read())
    up = sum(1 for m in data['members'] if m['status'] == 'up')
    print(f'  {up} nodes up')
    sys.exit(0 if up >= 3 else 1)
except Exception as e:
    print(f'  Not ready: {e}')
    sys.exit(1)
\" 2>/dev/null; then
        echo 'Cluster healthy!'
        break
    fi
    if [ \$i -eq 60 ]; then
        echo 'ERROR: Cluster not healthy after 10 minutes'
        exit 1
    fi
    sleep 10
done

# Run the load test (--no-project: no pyproject.toml needed on load generator)
cd ~
uv run --no-project --python 3.13 --with httpx --with asyncssh python -u -m loadtest --target-url $ALB_URL --ssh-key $REMOTE_KEY --ssh-user $SSH_USER ${LOADTEST_ARGS[@]+"${LOADTEST_ARGS[@]}"}
"

# --- 5. Fetch report + logs ---
echo ""
echo "=== Step 5/5: Fetching report + logs ==="
scp $SSH_OPTS "$SSH_USER@$LOADGEN_IP:~/loadtest/report-*.json" "$LOADTEST_DIR/" 2>/dev/null && \
    echo "Report saved to $LOADTEST_DIR/" || \
    echo "No JSON report found (test may have been interrupted)"

scp $SSH_OPTS "$SSH_USER@$LOADGEN_IP:~/loadtest/loadtest-*.log" "$LOADTEST_DIR/" 2>/dev/null && \
    echo "Logs saved to $LOADTEST_DIR/" || \
    echo "No log files found"

echo ""
echo "=== Done ==="

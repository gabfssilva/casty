#!/bin/bash
# Smoke test for Casty AWS cluster
# Usage: ./scripts/test_cluster.sh <ALB_URL>
# Example: ./scripts/test_cluster.sh http://casty-alb-123456.us-east-1.elb.amazonaws.com

set -euo pipefail

BASE_URL="${1:?Usage: $0 <ALB_URL>}"
BASE_URL="${BASE_URL%/}"  # strip trailing slash

echo "=== Casty AWS Cluster Smoke Test ==="
echo "Target: $BASE_URL"
echo ""

# Health check
echo "--- Health Check ---"
http GET "$BASE_URL/health"
echo ""

# Cluster status
echo "--- Cluster Status ---"
http GET "$BASE_URL/cluster/status"
echo ""

# Counter operations
echo "--- Counter: Increment 'order-1' by 5 ---"
http POST "$BASE_URL/counter/order-1/increment" amount:=5
echo ""

echo "--- Counter: Increment 'order-1' by 3 ---"
http POST "$BASE_URL/counter/order-1/increment" amount:=3
echo ""

echo "--- Counter: Get 'order-1' (expect 8) ---"
http GET "$BASE_URL/counter/order-1"
echo ""

echo "--- Counter: Increment 'order-2' by 10 ---"
http POST "$BASE_URL/counter/order-2/increment" amount:=10
echo ""

echo "--- Counter: Get 'order-2' (expect 10) ---"
http GET "$BASE_URL/counter/order-2"
echo ""

# KV operations
echo "--- KV: Put 'user-1' key 'name' = 'Alice' ---"
http PUT "$BASE_URL/kv/user-1/name" value="Alice"
echo ""

echo "--- KV: Get 'user-1' key 'name' (expect Alice) ---"
http GET "$BASE_URL/kv/user-1/name"
echo ""

echo "--- KV: Put 'user-1' key 'email' = 'alice@example.com' ---"
http PUT "$BASE_URL/kv/user-1/email" value="alice@example.com"
echo ""

echo "--- KV: Get 'user-1' key 'email' (expect alice@example.com) ---"
http GET "$BASE_URL/kv/user-1/email"
echo ""

echo "=== All tests passed ==="

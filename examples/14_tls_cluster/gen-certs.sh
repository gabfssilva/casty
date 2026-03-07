#!/usr/bin/env bash
set -euo pipefail

DIR="${1:-/certs}"
mkdir -p "$DIR"

echo "Creating CA..."
casty cert create-ca --out "$DIR" --force

for i in 1 2 3 4 5; do
    echo "Creating cert for node-$i..."
    casty cert create-node "node-$i" localhost 127.0.0.1 \
        --ca-dir "$DIR" --out "$DIR/node-$i" --force
done

echo "Done. Certificates in $DIR:"
find "$DIR" -name '*.crt' -o -name '*.key' | sort

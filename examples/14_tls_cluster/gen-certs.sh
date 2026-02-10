#!/usr/bin/env bash
set -euo pipefail

DIR="${1:-/certs}"
mkdir -p "$DIR"

echo "Generating CA..."
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$DIR/ca-key.pem" \
    -out "$DIR/ca.pem" \
    -days 365 \
    -subj "/CN=casty-ca" 2>/dev/null

for i in 1 2 3 4 5; do
    echo "Generating cert for node-$i..."

    cat > "$DIR/node-$i.cnf" <<EOF
[req]
distinguished_name = dn
req_extensions = v3_req
prompt = no

[dn]
CN = node-$i

[v3_req]
subjectAltName = DNS:node-$i, DNS:localhost, IP:127.0.0.1
EOF

    openssl req -newkey rsa:2048 -nodes \
        -keyout "$DIR/node-$i-key.pem" \
        -out "$DIR/node-$i.csr" \
        -config "$DIR/node-$i.cnf" 2>/dev/null

    openssl x509 -req \
        -in "$DIR/node-$i.csr" \
        -CA "$DIR/ca.pem" \
        -CAkey "$DIR/ca-key.pem" \
        -CAcreateserial \
        -out "$DIR/node-$i-cert.pem" \
        -days 365 \
        -extensions v3_req \
        -extfile "$DIR/node-$i.cnf" 2>/dev/null

    cat "$DIR/node-$i-cert.pem" "$DIR/node-$i-key.pem" > "$DIR/node-$i.pem"

    rm "$DIR/node-$i.cnf" "$DIR/node-$i.csr" "$DIR/node-$i-key.pem" "$DIR/node-$i-cert.pem"
done

rm -f "$DIR/ca-key.pem" "$DIR/ca.srl"

echo "Done. Certificates in $DIR:"
ls -1 "$DIR"

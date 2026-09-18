#!/bin/bash
# SEC bench PKI: one local CA, one server cert (SAN: 172.168.22.23, 127.0.0.1, localhost)
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f ca.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 60 \
    -subj "/CN=sec-bench-ca" 2>/dev/null
fi
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=172.168.22.23" 2>/dev/null
printf "[san]\nsubjectAltName=IP:172.168.22.23,IP:127.0.0.1,DNS:localhost\n" > san.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.pem -days 60 -extensions san -extfile san.cnf 2>/dev/null
echo CERTS_OK

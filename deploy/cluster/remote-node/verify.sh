#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
port="${REMOTE_PORT:-8002}"
curl --fail --silent --show-error "http://127.0.0.1:${port}/health"
echo

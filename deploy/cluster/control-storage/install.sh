#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
test -f .env || { echo "copy .env.example to .env and edit it first" >&2; exit 2; }
docker load -i images/fgac-polaris-minimal-1.7.0-demo.tar
docker load -i images/fgacdemo-storage-latest.tar
docker compose up -d
docker compose ps

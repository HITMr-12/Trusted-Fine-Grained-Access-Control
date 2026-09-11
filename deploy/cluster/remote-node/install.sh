#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
test -f .env || { echo "copy .env.example to .env and edit it first" >&2; exit 2; }
docker load -i images/fgac-remote-latest.tar
docker compose up -d
docker compose ps

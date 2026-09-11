#!/usr/bin/env bash
set -euo pipefail

catalog_url=${CATALOG_URL:-http://127.0.0.1:8181}
storage_url=${STORAGE_URL:-http://127.0.0.1:8003}
remote_url=${REMOTE_URL:-http://127.0.0.1:8002}

systemctl is-active --quiet fgac-polaris
systemctl is-active --quiet fgac-storage
systemctl is-active --quiet fgac-remote
curl --fail --silent --show-error "$catalog_url/q/health"
curl --fail --silent --show-error "$storage_url/health"
curl --fail --silent --show-error "$remote_url/health"
printf '\nnative server R verification passed\n'

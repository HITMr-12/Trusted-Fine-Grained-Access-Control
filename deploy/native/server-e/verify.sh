#!/usr/bin/env bash
set -euo pipefail

adapter_url=${ADAPTER_URL:-http://127.0.0.1:8004}
catalog_url=${CATALOG_URL:?set CATALOG_URL to node R}
remote_url=${REMOTE_SERVICE_URL:?set REMOTE_SERVICE_URL to node R}

systemctl is-active --quiet fgac-adapter-core
test -r /opt/fgac/plugins/fgac-spark-extension.jar
test -r "$(pg_config --pkglibdir)/fgac_pg.so"
curl --fail --silent --show-error "$adapter_url/health"
curl --fail --silent --show-error "$catalog_url/q/health"
curl --fail --silent --show-error "$remote_url/health"
printf '\nnative server E verification passed\n'

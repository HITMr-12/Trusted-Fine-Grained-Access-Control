#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker load -i images/spark-iceberg-latest.tar
docker load -i images/postgres-16-alpine.tar

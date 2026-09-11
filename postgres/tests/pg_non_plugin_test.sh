#!/bin/sh
set -eu

remote="${REMOTE_URL:-http://remote:8002}"
wget -qO /tmp/alice-orders.csv \
  --header='Authorization: Bearer alice-token' \
  "$remote/v2/virtual-tables/lake.sales.orders?columns=id,region,amount,owner&format=csv"
wget -qO /tmp/bob-orders.csv \
  --header='Authorization: Bearer bob-token' \
  "$remote/v2/virtual-tables/lake.sales.orders?columns=id,region,amount,owner&format=csv"

psql -X -v ON_ERROR_STOP=1 -f /tests/pg_non_plugin_test.sql

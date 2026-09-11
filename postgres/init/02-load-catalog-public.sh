#!/bin/sh
set -eu

metadata="$(wget -qO- "${POLARIS_URL:-http://polaris-fgac:8181}/v2/relations/public/departments")"
uri="$(printf '%s' "$metadata" | sed -n 's/.*"storage_uri":"\([^"]*\)".*/\1/p')"
case "$uri" in
  public://[A-Za-z0-9._-]*) file="${uri#public://}" ;;
  *) echo "Catalog returned unsupported direct URI: $uri" >&2; exit 1 ;;
esac

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
COPY departments (owner, department, enabled)
FROM '/public-data/$file' WITH (FORMAT csv, HEADER true);
SQL

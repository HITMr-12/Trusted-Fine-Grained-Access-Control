#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)

command -v mvn >/dev/null
command -v make >/dev/null
command -v pg_config >/dev/null

mvn --file "$repo_root/plugins/spark/pom.xml" -DskipTests package
install -m 0644 "$repo_root/plugins/spark/target/fgac-spark-extension-1.0.0.jar" \
  /opt/fgac/plugins/fgac-spark-extension.jar

make --directory "$repo_root/plugins/postgres" with_llvm=no
pg_libdir=$(pg_config --pkglibdir)
install -m 0755 "$repo_root/plugins/postgres/fgac_pg.so" "$pg_libdir/fgac_pg.so"

echo "installed Spark adapter in /opt/fgac/plugins and PostgreSQL adapter in $pg_libdir"

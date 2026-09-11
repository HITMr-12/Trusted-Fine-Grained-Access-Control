#!/usr/bin/env bash
set -euo pipefail

failed=false
if systemctl is-active --quiet fgac-adapter-core.service 2>/dev/null; then
  echo "active service remains: fgac-adapter-core" >&2
  failed=true
fi

for path in /opt/fgac /var/lib/fgac-adapter /var/log/fgac /etc/fgac \
  /etc/fgac/adapter.env /etc/fgac/engine.env \
  /etc/systemd/system/fgac-adapter-core.service; do
  if [[ -e "$path" ]]; then
    echo "path remains: $path" >&2
    failed=true
  fi
done

if command -v pg_config >/dev/null; then
  pg_plugin="$(pg_config --pkglibdir)/fgac_pg.so"
  if [[ -e "$pg_plugin" ]]; then
    echo "PostgreSQL plugin remains: $pg_plugin" >&2
    failed=true
  fi
fi

if pgrep -af 'uvicorn app:app.*adapter-core' >/dev/null; then
  echo "Adapter Core process remains" >&2
  pgrep -af 'uvicorn app:app.*adapter-core' >&2 || true
  failed=true
fi

if ss -H -ltn '( sport = :8004 )' | grep -q .; then
  echo "Adapter Core port remains listening" >&2
  ss -H -ltnp '( sport = :8004 )' >&2 || true
  failed=true
fi

$failed && exit 1
echo "node E cleanup verification passed"

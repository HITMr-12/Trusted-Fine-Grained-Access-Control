#!/usr/bin/env bash
set -euo pipefail

expect_data_removed=false
[[ ${1:-} != "--expect-data-removed" ]] || expect_data_removed=true

failed=false
for unit in fgac-remote fgac-storage fgac-polaris; do
  if systemctl is-active --quiet "$unit.service" 2>/dev/null; then
    echo "active service remains: $unit" >&2
    failed=true
  fi
done

for path in /opt/fgac /var/lib/fgac /var/log/fgac /etc/fgac \
  /etc/fgac/remote.env /etc/fgac/storage.env \
  /etc/systemd/system/fgac-remote.service \
  /etc/systemd/system/fgac-storage.service \
  /etc/systemd/system/fgac-polaris.service; do
  if [[ -e "$path" ]]; then
    echo "path remains: $path" >&2
    failed=true
  fi
done

if $expect_data_removed && [[ -e /srv/fgac/data ]]; then
  echo "data remains: /srv/fgac/data" >&2
  failed=true
fi

if pgrep -af 'uvicorn (remote|storage)\.app:app|org\.apache\.polaris\.fgac\.MinimalPolarisFgac' >/dev/null; then
  echo "FGAC process remains" >&2
  pgrep -af 'uvicorn (remote|storage)\.app:app|org\.apache\.polaris\.fgac\.MinimalPolarisFgac' >&2 || true
  failed=true
fi

if ss -H -ltn '( sport = :8002 or sport = :8003 or sport = :8181 )' | grep -q .; then
  echo "FGAC service port remains listening" >&2
  ss -H -ltnp '( sport = :8002 or sport = :8003 or sport = :8181 )' >&2 || true
  failed=true
fi

$failed && exit 1
echo "node R cleanup verification passed"

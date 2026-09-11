#!/usr/bin/env bash
set -euo pipefail

execute=false
purge_results=false
purge_users=false

usage() {
  echo "usage: $0 [--purge-results] [--purge-users] [--yes]"
}

while (($#)); do
  case "$1" in
    --purge-results) purge_results=true ;;
    --purge-users) purge_users=true ;;
    --yes) execute=true ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

if [[ ${EUID} -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)

pg_plugin=""
if command -v pg_config >/dev/null; then
  pg_plugin="$(pg_config --pkglibdir)/fgac_pg.so"
fi

echo "node E cleanup plan:"
echo "  stop and remove fgac-adapter-core"
echo "  remove /etc/fgac service and engine environments"
echo "  remove /opt/fgac, /var/lib/fgac-adapter, /var/log/fgac and FGAC private tmp"
[[ -n "$pg_plugin" ]] && echo "  remove $pg_plugin"
$purge_results && echo "  remove /srv/fgac/results"
$purge_users && echo "  remove fgac-adapter user and group"

if ! $execute; then
  echo "dry run only; repeat with --yes to execute"
  exit 0
fi

remove_exact_tree() {
  local requested=$1 expected=$2 resolved
  resolved=$(realpath -m -- "$requested")
  if [[ "$resolved" != "$expected" ]]; then
    echo "refusing unexpected removal target: $resolved" >&2
    exit 3
  fi
  [[ ! -e "$resolved" ]] || rm -rf --one-file-system -- "$resolved"
}

if [[ -n "$pg_plugin" && -e "$pg_plugin" ]]; then
  mapped=$(grep -l -F -- "$pg_plugin" /proc/[0-9]*/maps 2>/dev/null || true)
  if [[ -n "$mapped" ]]; then
    echo "PostgreSQL plugin is still mapped by a process; close benchmark sessions first:" >&2
    echo "$mapped" >&2
    exit 4
  fi
fi

systemctl disable --now fgac-adapter-core.service 2>/dev/null || true
rm -f -- \
  /etc/systemd/system/fgac-adapter-core.service \
  /etc/fgac/adapter.env \
  /etc/fgac/engine.env
rmdir /etc/fgac 2>/dev/null || true

[[ -z "$pg_plugin" ]] || rm -f -- "$pg_plugin"
remove_exact_tree /opt/fgac /opt/fgac
remove_exact_tree /var/lib/fgac-adapter /var/lib/fgac-adapter
remove_exact_tree /var/log/fgac /var/log/fgac
$purge_results && remove_exact_tree /srv/fgac/results /srv/fgac/results
$purge_results && rmdir /srv/fgac 2>/dev/null || true

if [[ -d "$repo_root/.git" ]]; then
  remove_exact_tree "$repo_root/plugins/spark/target" "$repo_root/plugins/spark/target"
  rm -f -- \
    "$repo_root/plugins/postgres/fgac_pg.o" \
    "$repo_root/plugins/postgres/fgac_pg.so" \
    "$repo_root/plugins/postgres/fgac_pg.bc"
fi

for base in /tmp /var/tmp; do
  while IFS= read -r -d '' candidate; do
    resolved=$(realpath -m -- "$candidate")
    case "$resolved" in
      "$base"/systemd-private-*-fgac-*.service-*) rm -rf --one-file-system -- "$resolved" ;;
      *) echo "refusing unexpected temporary path: $resolved" >&2; exit 3 ;;
    esac
  done < <(find "$base" -maxdepth 1 -type d -name 'systemd-private-*-fgac-*.service-*' -print0 2>/dev/null)
done

if $purge_users; then
  id fgac-adapter >/dev/null 2>&1 && userdel fgac-adapter
  getent group fgac-adapter >/dev/null && groupdel fgac-adapter
fi

systemctl daemon-reload
systemctl reset-failed fgac-adapter-core.service 2>/dev/null || true
echo "node E product cleanup completed"

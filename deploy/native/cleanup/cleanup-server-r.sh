#!/usr/bin/env bash
set -euo pipefail

execute=false
purge_data=false
purge_users=false

usage() {
  echo "usage: $0 [--purge-data] [--purge-users] [--yes]"
}

while (($#)); do
  case "$1" in
    --purge-data) purge_data=true ;;
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

echo "node R cleanup plan:"
echo "  stop and remove fgac-polaris, fgac-storage, fgac-remote"
echo "  remove /etc/fgac service environments"
echo "  remove /opt/fgac, /var/lib/fgac, /var/log/fgac and FGAC private tmp"
$purge_data && echo "  remove /srv/fgac/data"
$purge_users && echo "  remove fgac user and group"

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

for unit in fgac-remote fgac-storage fgac-polaris; do
  systemctl disable --now "$unit.service" 2>/dev/null || true
done

rm -f -- \
  /etc/systemd/system/fgac-remote.service \
  /etc/systemd/system/fgac-storage.service \
  /etc/systemd/system/fgac-polaris.service \
  /etc/fgac/remote.env \
  /etc/fgac/storage.env
rmdir /etc/fgac 2>/dev/null || true

remove_exact_tree /opt/fgac /opt/fgac
remove_exact_tree /var/lib/fgac /var/lib/fgac
remove_exact_tree /var/log/fgac /var/log/fgac
$purge_data && remove_exact_tree /srv/fgac/data /srv/fgac/data
$purge_data && rmdir /srv/fgac 2>/dev/null || true

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
  id fgac >/dev/null 2>&1 && userdel fgac
  getent group fgac >/dev/null && groupdel fgac
fi

systemctl daemon-reload
systemctl reset-failed fgac-remote.service fgac-storage.service fgac-polaris.service 2>/dev/null || true
echo "node R product cleanup completed"

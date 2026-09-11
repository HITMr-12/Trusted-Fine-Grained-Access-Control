#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)

command -v python3 >/dev/null

getent group fgac-adapter >/dev/null || groupadd --system fgac-adapter
id fgac-adapter >/dev/null 2>&1 || useradd --system --gid fgac-adapter --home-dir /var/lib/fgac-adapter --create-home --shell /sbin/nologin fgac-adapter

install -d -o root -g fgac-adapter -m 0750 /opt/fgac /opt/fgac/plugins /etc/fgac
cp -a "$repo_root/adapter-core" /opt/fgac/

python3 -m venv /opt/fgac/venv
/opt/fgac/venv/bin/pip install --requirement "$repo_root/deploy/native/requirements/adapter.txt"

install -m 0644 "$script_dir/fgac-adapter-core.service" /etc/systemd/system/fgac-adapter-core.service
[[ -f /etc/fgac/adapter.env ]] || install -m 0600 "$script_dir/fgac-adapter.env.example" /etc/fgac/adapter.env
[[ -f /etc/fgac/engine.env ]] || install -m 0600 "$script_dir/fgac-engine.env.example" /etc/fgac/engine.env

chown -R root:fgac-adapter /opt/fgac
chmod -R o-rwx /opt/fgac
systemctl daemon-reload
echo "edit /etc/fgac/adapter.env and /etc/fgac/engine.env, then build the plugins"

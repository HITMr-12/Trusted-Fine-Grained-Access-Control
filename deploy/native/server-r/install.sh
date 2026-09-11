#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)

command -v python3 >/dev/null
command -v javac >/dev/null
command -v java >/dev/null

getent group fgac >/dev/null || groupadd --system fgac
id fgac >/dev/null 2>&1 || useradd --system --gid fgac --home-dir /var/lib/fgac --create-home --shell /sbin/nologin fgac

install -d -o root -g fgac -m 0750 /opt/fgac /opt/fgac/catalog /etc/fgac
install -d -o fgac -g fgac -m 0750 /srv/fgac/data /var/lib/fgac

cp -a "$repo_root/remote" /opt/fgac/
cp -a "$repo_root/storage" /opt/fgac/

python3 -m venv /opt/fgac/venv
/opt/fgac/venv/bin/pip install --requirement "$repo_root/deploy/native/requirements/remote-data.txt"

install -d -o root -g fgac -m 0750 /opt/fgac/catalog/classes
find "$repo_root/catalog/polaris-minimal/src" -name '*.java' -print0 \
  | xargs -0 javac --add-modules jdk.httpserver -d /opt/fgac/catalog/classes

install -m 0644 "$script_dir/fgac-polaris.service" /etc/systemd/system/fgac-polaris.service
install -m 0644 "$script_dir/fgac-storage.service" /etc/systemd/system/fgac-storage.service
install -m 0644 "$script_dir/fgac-remote.service" /etc/systemd/system/fgac-remote.service

[[ -f /etc/fgac/storage.env ]] || install -m 0600 "$script_dir/fgac-storage.env.example" /etc/fgac/storage.env
[[ -f /etc/fgac/remote.env ]] || install -m 0600 "$script_dir/fgac-remote.env.example" /etc/fgac/remote.env

chown -R root:fgac /opt/fgac
chmod -R o-rwx /opt/fgac
systemctl daemon-reload
echo "edit /etc/fgac/storage.env and /etc/fgac/remote.env, provision /srv/fgac/data, then enable the services"

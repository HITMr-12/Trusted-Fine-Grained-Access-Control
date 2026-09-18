#!/bin/sh
# Launch a MASK v2 server instance with a given worker count / sec mode / port.
# Usage: start_mask_w.sh WORKERS on|off PORT
S=/data1/lyb/fgac-lab/runs/sec-bench-0918
W=$1
SEC=$2
PORT=$3
cd "$S" || exit 1
mkdir -p "$S/state-w$W-$SEC" "$S/logs"
ARGS="--source-manifest /data1/lyb/fgac-lab/runs/review-mask-v2-20260918/source/source-manifest.json \
--state-dir $S/state-w$W-$SEC --catalog-url http://127.0.0.1:18186 \
--host 172.168.22.23 --port $PORT --sec $SEC --workers $W"
if [ "$SEC" = "on" ]; then
  ARGS="$ARGS --cert $S/certs/server.pem --key $S/certs/server.key"
fi
exec python3 "$S/mask_server.py" $ARGS >> "$S/logs/mask_w${W}_${SEC}.log" 2>&1

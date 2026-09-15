#!/bin/sh
set -eu
FGAC_LAB_ROOT=/home/lyb/fgac-lab
. "$FGAC_LAB_ROOT/activate.sh"
case "${1:-}" in
 native) FGAC_CONF=native-conf ;;
 inline) FGAC_CONF=inline-conf ;;
 fgac) FGAC_CONF=conf ;;
 *) echo 'Usage: spark-current.sh native|inline|fgac job.py [args...]' >&2; exit 2 ;;
esac
shift
export SPARK_CONF_DIR="$FGAC_LAB_ROOT/deploy/fgac-current/$FGAC_CONF"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PYSPARK_PYTHON=/home/lyb/fgac/venv/bin/python
export PYTHONPATH="$FGAC_LAB_ROOT/envs/fgac-python:$FGAC_LAB_ROOT/deploy/fgac-current"
exec /home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit "$@"

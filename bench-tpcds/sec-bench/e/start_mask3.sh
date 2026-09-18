#!/bin/sh
# Launch MASK3 fused-variant resident worker (E side).
S=/home/lyb/fgac-lab/runs/sec-bench-0918
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export SPARK_HOME=/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3
export SPARK_CONF_DIR=/home/lyb/fgac-lab/runs/review-mask-v2-20260918/runtime/conf
export PYSPARK_PYTHON=/home/lyb/fgac/venv/bin/python
export HADOOP_USER_NAME=lyb
export SEC_CA=$S/ca.pem
export MASK_HOST=172.168.22.23
export MASK3_DELIVERY_ROOT=/dev/shm/sec-mask3
TS="-Djavax.net.ssl.trustStore=$S/truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit"
mkdir -p /dev/shm/sec-mask3 "$S/logs"
exec "$SPARK_HOME/bin/spark-submit" --conf "spark.driver.extraJavaOptions=$TS" \
  "$S/sec_worker.py" MASK3 lyb "$S/out" >> "$S/logs/worker_mask3.log" 2>&1

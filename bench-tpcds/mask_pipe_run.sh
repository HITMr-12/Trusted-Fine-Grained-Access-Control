#!/bin/bash
# True E/R-concurrent MASK pipeline runner (one case).
#   E host (172.168.22.25, ssh :10025): Spark pipeline_e.py polls pipe dir;
#                                        nc listener -> stream_recv.py writes files.
#   R host (172.168.22.23, ssh :10023): mask_e2e_producer.py stream | nc -> E.
# R production, LAN transfer and E consumption overlap in one wall window.
# Usage: mask_pipe_run.sh <label> <thr|NONE> <expr> <port> [batch_size]
set -u
LABEL=$1; THR=$2; EXPR=$3; PORT=$4; BATCH=${5:-4}

export SSH_ASKPASS="D:\Kimi\WorkSpace\可信细粒度访问控制\ssh-askpass.sh"
export SSH_ASKPASS_REQUIRE=force DISPLAY=:0
SSH="ssh -o StrictHostKeyChecking=no"
E_HOST="lyb@140.210.239.53 -p 10025"
R_HOST="lyb@140.210.239.53 -p 10023"
EDIR=/home/lyb/fgac-lab/runs/mask-e2e
RDIR=/data1/lyb/fgac-lab/runs/mask-e2e
PIPE=$EDIR/pipe_v2_$LABEL
LOGD=pipe_logs_$LABEL

# 0) clean E pipe dir + local log dir
$SSH -p 10025 lyb@140.210.239.53 "rm -rf $PIPE && mkdir -p $PIPE" < /dev/null || exit 1
rm -rf $LOGD && mkdir -p $LOGD

# 1) E: Spark consumer (persistent session, fgac-current conf = baseline config)
$SSH -p 10025 lyb@140.210.239.53 "
  export SPARK_CONF_DIR=/home/lyb/fgac-lab/deploy/fgac-current/conf \
         JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64 \
         PYSPARK_PYTHON=/home/lyb/fgac/venv/bin/python \
         PYTHONPATH=/home/lyb/fgac-lab/deploy/fgac-tpcds-bench:/home/lyb/fgac-lab/envs/fgac-python:/home/lyb/fgac-lab/deploy/fgac-current \
         HADOOP_USER_NAME=lyb SPARK_LOCAL_DIRS=/home/lyb/fgac-lab/tmp TMPDIR=/home/lyb/fgac-lab/tmp
  /home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit \
    $EDIR/pipeline_e.py $PIPE '$EXPR' $LABEL $BATCH
" < /dev/null > $LOGD/e_pipe.log 2>&1 &
E_PIPE_PID=$!

# 2) E: nc listener -> stream_recv (atomic publish per file)
$SSH -p 10025 lyb@140.210.239.53 "nc -l -p $PORT | python3 $EDIR/stream_recv.py $PIPE" \
  < /dev/null > $LOGD/e_recv.log 2>&1 &
E_RECV_PID=$!
sleep 2   # let the listener bind

# 3) R: streaming producer -> nc direct to E (LAN)
T0=$(date +%s%N)
$SSH -p 10023 lyb@140.210.239.53 "cd $RDIR && python3 mask_e2e_producer.py stream $LABEL $THR 2>$RDIR/prod_stream_$LABEL.log | nc -N 172.168.22.25 $PORT" \
  < /dev/null > $LOGD/r_prod.log 2>&1
R_RC=$?
T1=$(date +%s%N)
echo "R_STREAM rc=$R_RC wall_ms=$(( (T1-T0)/1000000 ))"

# 4) wait for E consumer to drain (its own .done-driven exit)
wait $E_PIPE_PID; E_RC=$?
T2=$(date +%s%N)
wait $E_RECV_PID
echo "PIPE_E2E rc=$E_RC wall_ms_from_R_start=$(( (T2-T0)/1000000 ))"
echo "== E consumer =="; grep -E 'PIPE_STAGE|PIPE ' $LOGD/e_pipe.log | tail -15
echo "== E recv tail =="; tail -2 $LOGD/e_recv.log
echo "== R prod tail =="; $SSH -p 10023 lyb@140.210.239.53 "tail -3 $RDIR/prod_stream_$LABEL.log" < /dev/null

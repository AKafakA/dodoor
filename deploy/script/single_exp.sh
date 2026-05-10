#!/bin/bash

BETA=$1
BATCH_SIZE=$2
CPU_WEIGHT=$3
DURATION_WEIGHT=$4
DATA_PATH=$5
SCHEDULER=$6
BRANCH_NAME=$7
REBUILD=$8
LOG_DIR_PREFIX=${9}

STATIC_CONFIG_PATH=${10}
HOST_CONFIG_PATH=${11}
TASK_CONFIG_PATH=${12}

NUM_REQUESTS=${13}
CODE_UPDATE=${14}
RUN_EXPERIMENT=${15}
EXPERIMENT_TIMEOUT_IN_MIN=${16}
QPS=${17}
RESTRICT_FIFO=${18}
ENABLE_BACKGROUND_QUERY=${19}
DEBUG_LOGS=${20}
LOG_LEVEL=${21}

WARMUP=${22}
WARMUP_REQUESTS=${23}
WARMUP_QPS=${24}
WARMUP_TRACE=${25}

ENABLE_PER_TASK_LOGS=${26}


CL_USER="${CLOUDLAB_USER:-${USER:-asdwb}}"

if [ "$CODE_UPDATE" = "true" ]; then
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sudo chown -R ${CL_USER} .git"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git config --global --add safe.directory /users/${CL_USER}/dodoor"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git fetch -a"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout $BRANCH_NAME && git reset --hard HEAD~20 && git pull"
fi

if [ "$REBUILD" = "true" ]; then
  echo "Rebuilding the project..."
  parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host "cd dodoor && sh rebuild.sh"
else
  echo "Skipping rebuild step."
fi

# Clean previous combo's logs but PRESERVE .err files: if a service crashes,
# its .err is the only diagnostic we have, and it must survive the next
# combo's startup rm so we can fetch it via collect_logs (DEBUG_LOGS=true)
# or directly via ssh after the failure.
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "rm -f ~/*.log ~/*.out" > /dev/null 2>&1
sleep 5
#parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
bash deploy/script/test_cloudlab.sh $SCHEDULER $BATCH_SIZE $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT $HOST_CONFIG_PATH $TASK_CONFIG_PATH $STATIC_CONFIG_PATH $NUM_REQUESTS $RUN_EXPERIMENT $EXPERIMENT_TIMEOUT_IN_MIN $QPS $RESTRICT_FIFO $ENABLE_BACKGROUND_QUERY $LOG_LEVEL $WARMUP $WARMUP_REQUESTS $WARMUP_QPS $WARMUP_TRACE $ENABLE_PER_TASK_LOGS

sleep 5
if [ "$RUN_EXPERIMENT" = "true" ]; then
  echo "Experiment run completed. Collecting logs..."
  export PYTHONPATH=~/Code/scheduling/dodoor
  python3 deploy/python/scripts/collect_logs.py $SCHEDULER $BATCH_SIZE $BETA $CPU_WEIGHT $DURATION_WEIGHT $LOG_DIR_PREFIX $QPS $DEBUG_LOGS > /dev/null 2>&1
fi

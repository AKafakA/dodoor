#!/bin/bash

BETA=$1
BATCH_SIZE=$2
CPU_WEIGHT=$3
DURATION_WEIGHT=$4
AVG_CLUSTER_LOAD=$5
DATA_PATH=$6
SCHEDULER=$7
BRANCH_NAME=$8
REBUILD=$9
LOG_DIR_PREFIX=${10}

STATIC_CONFIG_PATH=${11}
HOST_CONFIG_PATH=${12}
TASK_CONFIG_PATH=${13}

NUM_REQUESTS=${14}
CODE_UPDATE=${15}
RUN_EXPERIMENT=${16}

if [ "$CODE_UPDATE" = "true" ]; then
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout $BRANCH_NAME && git reset --hard HEAD~10 && git pull"
fi

if [ "$REBUILD" = "true" ]; then
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sh rebuild.sh"
else
  echo "Skipping rebuild step."
fi

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
sh deploy/script/test_cloudlab.sh $SCHEDULER $BATCH_SIZE $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT $AVG_CLUSTER_LOAD $HOST_CONFIG_PATH $TASK_CONFIG_PATH $STATIC_CONFIG_PATH $NUM_REQUESTS $RUN_EXPERIMENT

if [ "$RUN_EXPERIMENT" = "true" ]; then
  echo "Experiment run completed. Collecting logs..."
  export PYTHONPATH=~/Code/scheduling/dodoor
  python3 deploy/python/scripts/collect_logs.py $SCHEDULER $BATCH_SIZE $BETA $CPU_WEIGHT $DURATION_WEIGHT $AVG_CLUSTER_LOAD $LOG_DIR_PREFIX
else
  echo "Skipping log collection as RUN_EXPERIMENT is set to false."
fi
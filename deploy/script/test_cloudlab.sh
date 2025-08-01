#!/bin/bash

SCHEDULER_NUM_TASKS_UPDATE=4
SCHEDULER_TYPE=$1
BATCH_SIZE=$2
BETA=$3
CPU_WEIGHT=$4
NETWORK_INTERFACE="enp1s0"
DATA_PATH=$5
DURATION_WEIGHT=$6
AVG_CLUSTER_LOAD=$7

HOST_CONFIG_PATH=$8
TASK_CONFIG_PATH=$9
STATIC_CONFIG_PATH=${10}
NUM_REQUESTS=${11}
RUN_EXPERIMENT=${12}
EXPERIMENT_TIMEOUT_IN_MIN=${13}
QPS=${14}

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f dodoor"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f stress"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f docker"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i  "cd dodoor && sudo python3 deploy/python/scripts/config_generator.py --replay_with_delay True --batch-size $BATCH_SIZE --beta $BETA --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE --network_interface $NETWORK_INTERFACE --cpu_weight $CPU_WEIGHT --duration_weight $DURATION_WEIGHT --cluster_avg_load $AVG_CLUSTER_LOAD"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_nodes -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d false -s false -n true  1>${SCHEDULER_TYPE}_node_service.out  2>${SCHEDULER_TYPE}_node_service.err &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d true -s true -n false  1>${SCHEDULER_TYPE}_scheduler_service.out 2>${SCHEDULER_TYPE}_scheduler_service.err &"

if [ "$RUN_EXPERIMENT" = "true" ]; then
  sleep 20
  echo "Starting the experiment run with QPS=${QPS} and NUM_REQUESTS=${NUM_REQUESTS} for scheduler type ${SCHEDULER_TYPE} with data path ${DATA_PATH}."
  parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -q ${QPS} -f dodoor/$DATA_PATH 1>${SCHEDULER_TYPE}_replay.out 2>${SCHEDULER_TYPE}_replay.err &"
  # Wait for the tasks to complete
  sleep 10
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i -t 0 "cd dodoor && export PYTHONPATH=. && python3 deploy/python/scripts/wait_for_task_completion.py --log ~/${SCHEDULER_TYPE}_scheduler_metrics.log --num_requests ${NUM_REQUESTS} --timeout ${EXPERIMENT_TIMEOUT_IN_MIN}"
fi
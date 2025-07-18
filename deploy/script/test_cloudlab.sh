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

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f dodoor"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f stress"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i  "sudo chmod -R 777 /users/asdwb/dodoor && cd dodoor && sudo python3 deploy/python/scripts/config_generator.py -d ~/cloud_lab/test_scheduler_ip -n ~/cloud_lab/test_node_ip  -s ~/cloud_lab/test_scheduler_ip --replay_with_delay True --use-configable-address True --batch-size $BATCH_SIZE --beta $BETA --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE --network_interface $NETWORK_INTERFACE --cpu_weight $CPU_WEIGHT --duration_weight $DURATION_WEIGHT --cluster_avg_load $AVG_CLUSTER_LOAD"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_nodes -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d false -s false -n true  1>${SCHEDULER_TYPE}_node_service.out  2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d true -s true -n false  1>${SCHEDULER_TYPE}_scheduler_service.out 2>/dev/null &"

if [ "$RUN_EXPERIMENT" = "true" ]; then
  sleep 20
  parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -f dodoor/$DATA_PATH 1>${SCHEDULER_TYPE}_replay.out 2>/dev/null &"
  # Wait for the tasks to complete
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "export PYTHONPATH=$HOME/Code/scheduling/dodoor && python3 wait_for_task_completion.py --log scheduler_metrics.log --num_requests ${NUM_REQUESTS}"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f dodoor"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f stress"
fi
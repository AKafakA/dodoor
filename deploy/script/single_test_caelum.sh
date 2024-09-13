#!/bin/bash

CORES=6
MEMORY=61440
SCHEDULER_PORTS=20504, 20505, 20506
SCHEDULER_NUM_TASKS_UPDATE=1

SCHEDULER_TYPE="sparrow"
BATCH_SIZE=1
SLOT_SIZE=2
BETA=1.0
CPU_WEIGHT=1.0
NETWORK_INTERFACE="eno1"
DATA_PATH="deploy/resources/data/azure_data_small"
DURATION_WEIGHT=0.5

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum  -i "rm ~/*.log && rm ~/*.out"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum -i "cd dodoor && git fetch --all && git add . && git stash && git checkout sparrow_impl && git reset --hard HEAD~1 && git pull && sh rebuild.sh"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum -i "pkill -f dodoor"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum  -i "pkill -f stress"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum -i  "cd dodoor && sudo python3 deploy/python/scripts/config_generator.py -d deploy/resources/host_addresses/caelum/caelum_scheduler_ip -n deploy/resources/host_addresses/caelum/small_caelum_host_ip  -s deploy/resources/host_addresses/caelum/caelum_scheduler_ip --replay_with_delay True --scheduler-ports $SCHEDULER_PORTS --use-configable-address True --batch-size $BATCH_SIZE --beta $BETA --cores $CORES --memory $MEMORY --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE --num-slots $SLOT_SIZE --network_interface $NETWORK_INTERFACE --cpu_weight $CPU_WEIGHT --duration_weight $DURATION_WEIGHT"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/small_node -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d false -s false -n true  1>service.out  2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d true -s true -n false  1>service.out 2>/dev/null &"

sleep 20

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c dodoor/config.conf -f dodoor/$DATA_PATH  1>replay.out 2>/dev/null &"
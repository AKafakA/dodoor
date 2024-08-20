#!/bin/bash

BATCH_SIZE=120
BETA=1
CORES=6
MEMORY=61440
SCHEDULER_PORTS=20503,20504,20505,20506,20507
SCHEDULER_NUM_TASKS_UPDATE=5
SCHEDULER_TYPE=$1

parallel-ssh -h deploy/resources/host_addresses/caelum/caelum  -i "pkill -f dodoor"

parallel-ssh -h deploy/resources/host_addresses/caelum/caelum  -i "pkill -f stress"

parallel-ssh -h deploy/resources/host_addresses/caelum/caelum -i  "cd dodoor && git add . && git stash && git checkout exp && git pull && python3 ~/dodoor/deploy/python/scripts/config_generator.py -d ~/dodoor/deploy/resources/host_addresses/caelum/caelum_scheduler_ip -n ~/dodoor/deploy/resources/host_addresses/caelum/caelum_host_ip  -s ~/dodoor/deploy/resources/host_addresses/caelum/caelum_scheduler_ip --replay_with_delay True --scheduler-ports $SCHEDULER_PORTS --use-configable-address True --batch-size $BATCH_SIZE --beta $BETA --cores $CORES --memory $MEMORY --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE && sh rebuild.sh && rm ~/*.log && rm ~/*.out"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/node -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d false -s false -n true  1>service.out  2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d true -s true -n false  1>service.out 2>/dev/null &"

sleep 50

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c dodoor/config.conf -f dodoor/deploy/resources/data/azure_data  1>replay.out 2>/dev/null &"
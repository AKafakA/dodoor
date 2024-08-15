#!/bin/bash

BATCH_SIZE=4
BETA=0.6
CORES=6
MEMORY=30720
SCHEDULER_PORTS=20503
SCHEDULER_TYPE=dodoor

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum  -i "pkill -f dodoor"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum  -i "pkill -f stress"

parallel-ssh -h deploy/resources/host_addresses/caelum/small_caelum -i  "cd dodoor && git add . && git stash && git checkout exp && git reset --hard HEAD~10 && git pull && python3 ~/dodoor/deploy/python/scripts/config_generator.py -d ~/dodoor/deploy/resources/host_addresses/caelum/caelum_scheduler_ip -n ~/dodoor/deploy/resources/host_addresses/caelum/small_caelum_host_ip  -s ~/dodoor/deploy/resources/host_addresses/caelum/caelum_scheduler_ip --replay_with_delay True --scheduler-ports $SCHEDULER_PORTS --use-configable-address True --batch-size $BATCH_SIZE --beta $BETA --cores $CORES --memory $MEMORY --scheduler-type $SCHEDULER_TYPE && sh rebuild.sh && rm ~/*.log && rm ~/*.out"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/small_node -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d false -s false -n true  1>service.out  2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d true -s true -n false  1>service.out 2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/caelum/scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c dodoor/config.conf -f dodoor/deploy/resources/data/azure_data_small  1>replay.out 2>/dev/null &"
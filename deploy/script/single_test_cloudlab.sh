#!/bin/bash

BATCH_SIZE=300
SLOT_SIZE=3
BETA=1
CORES=8
MEMORY=61440
SCHEDULER_PORTS=20503,20504,20505,20506,20507,20508,20509,20511,20512,20513
SCHEDULER_NUM_TASKS_UPDATE=30
SCHEDULER_TYPE=$1
NETWORK_INTERFACE="enp1s0"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_nodes -i "sudo chmod -R 777 /users/asdwb/dodoor/* & git config --global --add safe.directory /users/asdwb/dodoor && cd dodoor && git add . && git stash && git checkout exp && git pull && sh rebuild.sh && rm ~/*.log && rm ~/*.out"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f dodoor"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "pkill -f stress"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i  "sudo chmod -R 777 /users/asdwb/dodoor && cd dodoor && sudo python3 deploy/python/scripts/config_generator.py -d ~/cloud_lab/test_scheduler_ip -n ~/cloud_lab/test_node_ip  -s ~/cloud_lab/test_scheduler_ip --replay_with_delay True --scheduler-ports $SCHEDULER_PORTS --use-configable-address True --batch-size $BATCH_SIZE --beta $BETA --cores $CORES --memory $MEMORY --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE --num-slots $SLOT_SIZE --network_interface $NETWORK_INTERFACE && sh rebuild.sh && rm ~/*.log && rm ~/*.out"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_nodes -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d false -s false -n true  1>service.out  2>/dev/null &"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ~/dodoor/config.conf -d true -s true -n false  1>service.out 2>/dev/null &"

sleep 20

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c dodoor/config.conf -f dodoor/deploy/resources/data/azure_data_cloudlab  1>replay.out 2>/dev/null &"
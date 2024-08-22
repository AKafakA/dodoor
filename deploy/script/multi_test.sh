#!/bin/bash

SLEEP_TIME="10000"
BETA=1.0
BATCH_SIZE=100
CPU_WEIGHT=1.0

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && git config --global --add safe.directory /users/asdwb/dodoor && cd dodoor && git add . && git stash && git checkout exp && git pull && sh rebuild.sh"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
export PYTHONPATH=$PYTHONPATH:~/Code/scheduling/dodoor
for i in "sparrow" "random";
  do
  for slot_size in 3
    do
       echo "run the exp for $i"
       sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE $slot_size $BETA $CPU_WEIGHT
       sleep ${SLEEP_TIME}
       python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE $slot_size $BETA $CPU_WEIGHT
       parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
    done
done

for i in "cached_sparrow" "dodoor"
  do
  # shellcheck disable=SC2043
  for slot_size in 3
    do
    for batch_size in 100
      do
      for cpu_weight in 1.0 10.0 50.0 1000.0
        do
           echo "run the exp for $i with cpu weight $cpu_weight"
           sh deploy/script/single_test_cloudlab.sh $i $batch_size $slot_size $BETA $cpu_weight
           sleep ${SLEEP_TIME}
           python3 deploy/python/scripts/collect_logs.py $i $batch_size $slot_size $BETA $cpu_weight
           parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
        done
      done
    done
done

for i in "cached_sparrow" "dodoor"
  do
  # shellcheck disable=SC2043
  for slot_size in 3
    do
    for batch_size in 20 50
      do
      for cpu_weight in 10.0
        do
           echo "run the exp for $i with cpu weight $cpu_weight and batch size $batch_size"
           sh deploy/script/single_test_cloudlab.sh $i $batch_size $slot_size $BETA $cpu_weight
           sleep ${SLEEP_TIME}
           python3 deploy/python/scripts/collect_logs.py $i $batch_size $slot_size $BETA $cpu_weight
           parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
        done
      done
    done
done

python3 deploy/python/scripts/plot.py

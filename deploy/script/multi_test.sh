#!/bin/bash

SLEEP_TIME="15000"
BETA=1.0
BATCH_SIZE=100
CPU_WEIGHT=10.0
SLOT_SIZE=4
DURATION_WEIGHT=0.5
DATA_PATH="deploy/resources/data/azure_data_cloudlab_10m"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && git config --global --add safe.directory /users/asdwb/dodoor && cd dodoor && git add . && git stash && git checkout exp && git pull && sh rebuild.sh"
export PYTHONPATH=$PYTHONPATH:~/Code/scheduling/dodoor
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
rm -rf deploy/resources/log/*

for i in "sparrow" "asyncSparrow" "random" "dodoor" "cachedSparrow" "prequal";
  do
    echo "run the exp for $i"
    sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE $SLOT_SIZE $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT
    sleep ${SLEEP_TIME}
    python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE $SLOT_SIZE $BETA $CPU_WEIGHT $DURATION_WEIGHT
    sleep 60
done

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
SLOT_SIZE=3
for i in "sparrow" "asyncSparrow" "random" "dodoor" "cachedSparrow" "prequal";
  do
    echo "run the exp for $i"
    sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT
    sleep ${SLEEP_TIME}
    python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DURATION_WEIGHT
    sleep 60
done

#python3 deploy/python/scripts/plot.py
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
for i in "dodoor";
  do
  # shellcheck disable=SC2043
  for slot_size in 4
    do
    for batch_size in 100
      do
      for cpu_weight in 10.0
        do
          for duration_weight in 0.6 0.7 0.8
            do
              echo "run the exp for $i with cpu weight $cpu_weight and duration weight $duration_weight"
              sh deploy/script/single_test_cloudlab.sh $i $batch_size $slot_size $BETA $cpu_weight $DATA_PATH $duration_weight
              sleep ${SLEEP_TIME}
              python3 deploy/python/scripts/collect_logs.py $i $batch_size $slot_size $BETA $cpu_weight $duration_weight
              parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && mkdir -p ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight && mv ~/*.log ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/. & mv ~/*.out ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/."
            done
        done
      done
    done
done

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
for i in "dodoor";
  do
  # shellcheck disable=SC2043
  for slot_size in 4
    do
    for batch_size in 100
      do
      for cpu_weight in 2.0 5.0 20.0
        do
          for duration_weight in 0.5
            do
              echo "run the exp for $i with cpu weight $cpu_weight and duration weight $duration_weight"
              sh deploy/script/single_test_cloudlab.sh $i $batch_size $slot_size $BETA $cpu_weight $DATA_PATH $duration_weight
              sleep ${SLEEP_TIME}
              python3 deploy/python/scripts/collect_logs.py $i $batch_size $slot_size $BETA $cpu_weight $duration_weight
              parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && mkdir -p ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight && mv ~/*.log ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/. & mv ~/*.out ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/."
            done
        done
      done
    done
done
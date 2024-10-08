#!/bin/bash

SLEEP_TIME="14400"
BETA=1.0
BATCH_SIZE=100
CPU_WEIGHT=10.0
SLOT_SIZE=4
DURATION_WEIGHT=0.5
NUM_SCHEDULER=5
DATA_PATH="deploy/resources/data/azure_data_cloudlab_10m"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && git config --global --add safe.directory /users/asdwb/dodoor && cd dodoor && git add . && git stash && git checkout sparrow_impl && git pull && sh rebuild.sh"
export PYTHONPATH=$PYTHONPATH:~/Code/scheduling/dodoor
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
rm -rf deploy/resources/log/*

for i in "sparrow" "powerOfTwo" "powerOfTwoDuration" "prequal" "random" "dodoor" "cachedPowerOfTwo";
  do
    echo "run the exp for $i"
    sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE $SLOT_SIZE $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT $NUM_SCHEDULER
    sleep ${SLEEP_TIME}
    python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE $SLOT_SIZE $BETA $CPU_WEIGHT $DURATION_WEIGHT $NUM_SCHEDULER
done

#parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
#SLOT_SIZE=3
#NUM_SCHEDULER=5
#for i in "sparrow" "prequal" "random" "dodoor" "cachedSparrow";
#  do
#    echo "run the exp for $i"
#    sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT $NUM_SCHEDULER
#    sleep ${SLEEP_TIME}
#    python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DURATION_WEIGHT $NUM_SCHEDULER
#done
#
#parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
#SLOT_SIZE=4
#NUM_SCHEDULER=10
#for i in "sparrow" "prequal" "random" "dodoor" "cachedSparrow";
#  do
#    echo "run the exp for $i"
#    sh deploy/script/single_test_cloudlab.sh $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DATA_PATH $DURATION_WEIGHT $NUM_SCHEDULER
#    sleep ${SLEEP_TIME}
#    python3 deploy/python/scripts/collect_logs.py $i $BATCH_SIZE 3 $BETA $CPU_WEIGHT $DURATION_WEIGHT $NUM_SCHEDULER
#done
#
#parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "rm ~/*.log && rm ~/*.out"
#SLOT_SIZE=4
#NUM_SCHEDULER=5
#for i in "dodoor";
#  do
#  # shellcheck disable=SC2043
#  for slot_size in 4
#    do
#    for batch_size in 100
#      do
#      for cpu_weight in 10.0
#        do
#          for duration_weight in 0.55 0.6
#            do
#              echo "run the exp for $i with cpu weight $cpu_weight and duration weight $duration_weight"
#              sh deploy/script/single_test_cloudlab.sh $i $batch_size $slot_size $BETA $cpu_weight $DATA_PATH $duration_weight $NUM_SCHEDULER
#              sleep ${SLEEP_TIME}
#              python3 deploy/python/scripts/collect_logs.py $i $batch_size $slot_size $BETA $cpu_weight $duration_weight $NUM_SCHEDULER
#              parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -i "sudo chmod -R 777 /users/asdwb/dodoor && mkdir -p ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight && mv ~/*.log ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/. & mv ~/*.out ~/dodoor/deploy/resources/log/$i-$batch_size-$slot_size-$BETA-$cpu_weight-$duration_weight/."
#            done
#        done
#      done
#    done
#done
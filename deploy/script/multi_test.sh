#!/bin/bash

SLEEP_TIME="15000"
export PYTHONPATH=$PYTHONPATH:~/Code/scheduling/dodoor
for i in "sparrow" "random" "cached_sparrow";
do
   echo "run the exp for $i"
   sh deploy/script/single_test_cloudlab.sh "$i"
   sleep ${SLEEP_TIME}
   python3 deploy/python/scripts/collect_logs.py $i
done
   python3 deploy/python/scripts/plot.py

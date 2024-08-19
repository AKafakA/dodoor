#!/bin/bash

SLEEP_TIME="11000"
for i in "sparrow" "random" "cached_sparrow" "dodoor" "reverse_dodoor";
do
   echo "run the exp for $i"
   sh deploy/script/single_test_cloudlab.sh "$i"
   sleep ${SLEEP_TIME}
   python3 deploy/python/scripts/collect_logs.py $i
done
python3 deploy/python/scripts/plot.py

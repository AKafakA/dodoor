#!/bin/bash

for i in "dodoor" "sparrow" "random" "cached_sparrow";
do
   sh deploy/script/single_test_caelum.sh "$i"
   echo "run the exp for $i"
   sleep 5h
done

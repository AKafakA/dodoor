#!/bin/bash

for i in "dodoor" "sparrow" "random" "cached_sparrow";
do
   echo "run the exp for $i"
   sh deploy/script/single_test_caelum.sh "$i"
   sleep 8h
done

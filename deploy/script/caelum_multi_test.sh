#!/bin/bash
declare -a arr=("dodoor"
                "sparrow"
                "random"
                "cached_sparrow"
                )
for i in "${arr[@]}"
do
   sh deploy/script/single_test_caelum.sh i
   echo "run the exp for $i"
   sleep 5h
done

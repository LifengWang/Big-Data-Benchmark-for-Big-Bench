#!/bin/bash
for i in {10,11,18,19,27,29,30}
do
rm -rf queries/q$i
done


for i in {`seq 1 5`,8}
do
rm -rf queries/q0$i

done


#!/bin/bash

rm -rf log
mkdir log
# https://www.cyberciti.biz/faq/bash-for-loop/
for (( i=1; i<=3; i++ ))
do
   go test 1> log/out$i.txt 2> log/log$i.txt -v -race
   # go test -run TestUnreliableChurn2C 1> log/out$i.txt 2> log/log$i.txt -v -race
   # go test -run 2C 1> log/out$i.txt 2> log/log$i.txt -v -race
   echo "run $i result:"
   tail -n2 log/out$i.txt
done


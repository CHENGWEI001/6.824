#!/bin/bash

rm -rf log
mkdir log
# https://www.cyberciti.biz/faq/bash-for-loop/
for (( i=1; i<=1; i++ ))
do
   go test -run Challenge 1> log/out$i.txt 2> log/log$i.txt -v -race
   # go test 1> log/out$i.txt 2> log/log$i.txt -v -race
   echo "run $i result:" | tee -a log/report.txt
   tail -n2 log/out$i.txt | tee -a log/report.txt
done


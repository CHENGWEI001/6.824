#!/bin/bash

rm -rf log
mkdir log
# https://www.cyberciti.biz/faq/bash-for-loop/
for (( i=1; i<=5; i++ ))
do
   go test -run 3A 1> log/out$i.txt 2> log/log$i.txt -v -race -trace log/trace$i.out
   # go test -run TestBasic3A 1> log/out$i.txt 2> log/log$i.txt -v -race -trace log/trace$i.out
   # go test -run TestFigure8Unreliable2C 1> log/out$i.txt 2> log/log$i.txt -v -race
   # go test -run 2C 1> log/out$i.txt 2> log/log$i.txt -v -race
   echo "run $i result:" | tee -a log/report.txt
   tail -n2 log/out$i.txt | tee -a log/report.txt
done


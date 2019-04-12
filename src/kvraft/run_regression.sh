#!/bin/bash

rm -rf log
mkdir log
# https://www.cyberciti.biz/faq/bash-for-loop/
for (( i=1; i<=50; i++ ))
do
   # go test -run "TestSnapshotRPC3B|TestSnapshotSize3B" 1> log/out$i.txt 2> log/log$i.txt -v -race -trace log/trace$i.out
   # go test -run TestSnapshotRecover3B 1> log/out$i.txt 2> log/log$i.txt -v -race -trace log/trace$i.out
   # go test -run 3B 1> log/out$i.txt 2> log/log$i.txt -v -race
   go test 1> log/out$i.txt 2> log/log$i.txt -v -race
   # go test -run TestSnapshotSize3B 1> log/out$i.txt 2> log/log$i.txt -v -race
   echo "run $i result:" | tee -a log/report.txt
   tail -n2 log/out$i.txt | tee -a log/report.txt
done


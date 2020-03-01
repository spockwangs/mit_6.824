#! /bin/bash

LOG_FILE=/tmp/kvraft_test.log
rm -f ${LOG_FILE}
for ((i=0; i < 200; i++)); do
    echo "round $i" >> ${LOG_FILE}
    (go test -run TestManyPartitionsManyClients3A) >> ${LOG_FILE}
    grep -nr FAIL ${LOG_FILE}
done

#! /bin/bash

LOG_FILE=/tmp/shardkv_test.log
rm -f ${LOG_FILE}
for ((i=0; i < 200; i++)); do
    echo "round $i" >> ${LOG_FILE}
    (go test -run TestUnreliable1) >> ${LOG_FILE}
done

#! /bin/bash

LOG_FILE=/tmp/shardmaster_test.log
rm -f ${LOG_FILE}
for ((i=0; i < 200; i++)); do
    echo "round $i" >> ${LOG_FILE}
    (go test) >> ${LOG_FILE}
done

#! /bin/bash

LOG_FILE_PREFIX=/tmp/kvraft_test.log
CONCURRENCY=50
LOOP_CNT=4
for ((i=0; i < ${CONCURRENCY}; i++)); do
    LOG_FILE=${LOG_FILE_PREFIX}.$i
    rm -f ${LOG_FILE}
    for ((j=0; j < ${LOOP_CNT}; j++)); do
        ((k = i*LOOP_CNT + j))
        echo "round $k" >> ${LOG_FILE}
        (go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B) >> ${LOG_FILE}
    done&
done
wait

rm -f ${LOG_FILE_PREFIX}
for ((i=0; i < ${CONCURRENCY}; i++)) do
    cat ${LOG_FILE_PREFIX}.$i >> ${LOG_FILE_PREFIX}
    rm -f ${LOG_FILE_PREFIX}.$i
done
grep -nr FAIL ${LOG_FILE_PREFIX}

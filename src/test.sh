#! /bin/bash

rm -f /tmp/test.log
for ((i=0; i < 200; i++)); do
    echo "round $i" >> /tmp/test.log
    (go test -run TestChallenge1Concurrent) >> /tmp/test.log
    grep -nr "FAIL.*" /tmp/test.log
done

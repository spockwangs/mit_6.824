#! /bin/bash

rm -f /tmp/test.log
for ((i=0; i < 200; i++)); do
    echo $i
    (go test) >> /tmp/test.log
    grep -nr "FAIL.*" /tmp/test.log
done

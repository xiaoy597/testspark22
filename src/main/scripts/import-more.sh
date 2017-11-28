#!/bin/sh

export TOTAL=$1
export COUNT=0

while [ $COUNT -lt $TOTAL ]; do
        ./import-msg.sh 1>import.out.$COUNT 2>&1 &
        export COUNT=`expr $COUNT + 1`
done



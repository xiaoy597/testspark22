#!/bin/sh

INPUT_FILE=$1

if [ "$INPUT_FILE" = "" ]; then
	INPUT_FILE=csdc_s_sec_tran_tst.dat
fi

KAFKA_CONSOLE_PRODUCER=/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh


ARGS="--broker-list host2:6667 \
--max-memory-bytes 268435456 \
--max-partition-memory-bytes 1048576 \
--batch-size 400 \
--compression-codec gzip \
--request-required-acks 1 \
--topic sztran"

echo "Producer start at `date`"
$KAFKA_CONSOLE_PRODUCER $ARGS < $INPUT_FILE
echo "Producer stop at `date`"


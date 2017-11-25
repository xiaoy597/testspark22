#!/bin/sh

#kafka-topics.sh --delete --if-exists --zookeeper aisvr1:2181 --topic sztran
#kafka-topics.sh --create --zookeeper aisvr1:2181 --replication-factor 1 --partitions 12 --topic sztran
kafka-topics.sh --describe --zookeeper aisvr1:2181 --topic sztran

echo "`date`: sending messages ..."
kafka-console-producer.sh \
--broker-list aisvr2:6667 \
--topic sztran \
--request-required-acks 1 \
--batch-size 500 \
--compression-codec snappy < csdc_s_sec_tran_tst.dat
echo "`date`: done."

#kafka-topics.sh --describe --zookeeper aisvr1:2181 --topic sztran

kafka-run-class.sh kafka.tools.GetOffsetShell --topic sztran  --time -1 --broker-list aisvr2:6667 --partitions 0,1,2,3,4,5,6,7,8,9,10,11


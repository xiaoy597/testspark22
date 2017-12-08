#!/bin/sh
TOPIC=$1

kafka-topics.sh --describe --zookeeper host3:2181 --topic $TOPIC

kafka-run-class.sh kafka.tools.GetOffsetShell --topic $TOPIC  --time -1 --broker-list host2:6667 --partitions 0,1,2,3,4,5,6,7,8,9,10,11

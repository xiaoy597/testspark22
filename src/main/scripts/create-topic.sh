#!/bin/sh

TOPIC=$1

kafka-topics.sh --zookeeper host4:2181 --delete --topic $TOPIC

sleep 1

kafka-topics.sh --create --zookeeper host4:2181 --replication-factor 1 --partitions 12 --topic $TOPIC

kafka-topics.sh --describe --zookeeper host4:2181 --topic $TOPIC


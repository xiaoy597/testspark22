#!/bin/sh

#SCHEMA_NAME=$1
#TABLE_NAME=$2
#LOAD_MONTH=$3
#START_DATE=$4
#END_DATE=$5
#DEFAULT_END_DATE=$6
#
#if [ -z "$SCHEMA_NAME" -o -z "$TABLE_NAME" -o -z "$LOAD_MONTH" -o -z "START_DATE" -o -z "$END_DATE" -o -z "$DEFAULT_END_DATE" ]; then
#	echo "Insufficient program arguments."
#	exit 1
#fi
#
#export ETL_METADB_SERVER=192.168.112.128:3306
#export ETL_METADB_DBNAME=etl_metadata
#export ETL_METADB_USER=root
#export ETL_METADB_PASSWORD=root
#
#export ETL_ZIPPER_DEBUG=true
#export ETL_ZIPPER_PARTITION=4

DEPS="\
./lib/antlr4-runtime-4.5.3.jar,\
./lib/aopalliance-repackaged-2.4.0-b34.jar,\
./lib/avro-1.7.7.jar,\
./lib/avro-ipc-1.7.7.jar,\
./lib/avro-ipc-1.7.7-tests.jar,\
./lib/avro-mapred-1.7.7-hadoop2.jar,\
./lib/chill_2.11-0.8.0.jar,\
./lib/chill-java-0.8.0.jar,\
./lib/commons-beanutils-1.7.0.jar,\
./lib/commons-beanutils-core-1.8.0.jar,\
./lib/commons-cli-1.2.jar,\
./lib/commons-codec-1.3.jar,\
./lib/commons-collections-3.2.1.jar,\
./lib/commons-compiler-2.7.8.jar,\
./lib/commons-compress-1.4.1.jar,\
./lib/commons-configuration-1.6.jar,\
./lib/commons-digester-1.8.jar,\
./lib/commons-httpclient-3.1.jar,\
./lib/commons-io-2.1.jar,\
./lib/commons-lang-2.5.jar,\
./lib/commons-lang3-3.3.2.jar,\
./lib/commons-math-2.1.jar,\
./lib/commons-math3-3.4.1.jar,\
./lib/commons-net-2.2.jar,\
./lib/compress-lzf-1.0.3.jar,\
./lib/curator-client-2.4.0.jar,\
./lib/curator-framework-2.4.0.jar,\
./lib/curator-recipes-2.4.0.jar,\
./lib/guava-14.0.1.jar,\
./lib/hadoop-annotations-2.2.0.jar,\
./lib/hadoop-auth-2.2.0.jar,\
./lib/hadoop-client-2.2.0.jar,\
./lib/hadoop-common-2.2.0.jar,\
./lib/hadoop-hdfs-2.2.0.jar,\
./lib/hadoop-mapreduce-client-app-2.2.0.jar,\
./lib/hadoop-mapreduce-client-common-2.2.0.jar,\
./lib/hadoop-mapreduce-client-core-2.2.0.jar,\
./lib/hadoop-mapreduce-client-jobclient-2.2.0.jar,\
./lib/hadoop-mapreduce-client-shuffle-2.2.0.jar,\
./lib/hadoop-yarn-api-2.2.0.jar,\
./lib/hadoop-yarn-client-2.2.0.jar,\
./lib/hadoop-yarn-common-2.2.0.jar,\
./lib/hadoop-yarn-server-common-2.2.0.jar,\
./lib/hk2-api-2.4.0-b34.jar,\
./lib/hk2-locator-2.4.0-b34.jar,\
./lib/hk2-utils-2.4.0-b34.jar,\
./lib/ivy-2.4.0.jar,\
./lib/jackson-annotations-2.6.5.jar,\
./lib/jackson-core-2.6.5.jar,\
./lib/jackson-core-asl-1.9.13.jar,\
./lib/jackson-databind-2.6.5.jar,\
./lib/jackson-mapper-asl-1.9.13.jar,\
./lib/jackson-module-paranamer-2.6.5.jar,\
./lib/jackson-module-scala_2.11-2.6.5.jar,\
./lib/janino-2.7.8.jar,\
./lib/javassist-3.18.1-GA.jar,\
./lib/javax.annotation-api-1.2.jar,\
./lib/javax.inject-2.4.0-b34.jar,\
./lib/javax.servlet-api-3.1.0.jar,\
./lib/javax.ws.rs-api-2.0.1.jar,\
./lib/jcl-over-slf4j-1.7.16.jar,\
./lib/jersey-client-2.22.2.jar,\
./lib/jersey-common-2.22.2.jar,\
./lib/jersey-container-servlet-2.22.2.jar,\
./lib/jersey-container-servlet-core-2.22.2.jar,\
./lib/jersey-guava-2.22.2.jar,\
./lib/jersey-media-jaxb-2.22.2.jar,\
./lib/jersey-server-2.22.2.jar,\
./lib/jets3t-0.7.1.jar,\
./lib/jetty-util-6.1.26.jar,\
./lib/json4s-ast_2.11-3.2.11.jar,\
./lib/json4s-core_2.11-3.2.11.jar,\
./lib/json4s-jackson_2.11-3.2.11.jar,\
./lib/jsr305-1.3.9.jar,\
./lib/jul-to-slf4j-1.7.16.jar,\
./lib/kafka_2.11-0.8.2.1.jar,\
./lib/kafka-clients-0.8.2.1.jar,\
./lib/kryo-shaded-3.0.3.jar,\
./lib/leveldbjni-all-1.8.jar,\
./lib/log4j-1.2.17.jar,\
./lib/lz4-1.3.0.jar,\
./lib/mesos-0.21.1-shaded-protobuf.jar,\
./lib/metrics-core-2.2.0.jar,\
./lib/metrics-core-3.1.2.jar,\
./lib/metrics-graphite-3.1.2.jar,\
./lib/metrics-json-3.1.2.jar,\
./lib/metrics-jvm-3.1.2.jar,\
./lib/minlog-1.3.0.jar,\
./lib/netty-3.8.0.Final.jar,\
./lib/netty-all-4.0.29.Final.jar,\
./lib/objenesis-2.1.jar,\
./lib/oro-2.0.8.jar,\
./lib/osgi-resource-locator-1.0.1.jar,\
./lib/paranamer-2.6.jar,\
./lib/parquet-column-1.7.0.jar,\
./lib/parquet-common-1.7.0.jar,\
./lib/parquet-encoding-1.7.0.jar,\
./lib/parquet-format-2.3.0-incubating.jar,\
./lib/parquet-generator-1.7.0.jar,\
./lib/parquet-hadoop-1.7.0.jar,\
./lib/parquet-jackson-1.7.0.jar,\
./lib/protobuf-java-2.5.0.jar,\
./lib/py4j-0.10.1.jar,\
./lib/pyrolite-4.9.jar,\
./lib/RoaringBitmap-0.5.11.jar,\
./lib/scala-compiler-2.11.0.jar,\
./lib/scala-library-2.11.8.jar,\
./lib/scalap-2.11.0.jar,\
./lib/scala-parser-combinators_2.11-1.0.2.jar,\
./lib/scala-reflect-2.11.7.jar,\
./lib/scalatest_2.11-2.2.6.jar,\
./lib/scala-xml_2.11-1.0.2.jar,\
./lib/slf4j-api-1.7.12.jar,\
./lib/slf4j-log4j12-1.7.16.jar,\
./lib/snappy-java-1.1.2.4.jar,\
./lib/spark-catalyst_2.11-2.0.0.jar,\
./lib/spark-core_2.11-2.0.0.jar,\
./lib/spark-launcher_2.11-2.0.0.jar,\
./lib/spark-network-common_2.11-2.0.0.jar,\
./lib/spark-network-shuffle_2.11-2.0.0.jar,\
./lib/spark-sketch_2.11-2.0.0.jar,\
./lib/spark-sql_2.11-2.0.0.jar,\
./lib/spark-streaming_2.11-2.0.0.jar,\
./lib/spark-streaming-kafka-0-8_2.11-2.0.0.jar,\
./lib/spark-tags_2.11-2.0.0.jar,\
./lib/spark-unsafe_2.11-2.0.0.jar,\
./lib/stream-2.7.0.jar,\
./lib/univocity-parsers-2.1.1.jar,\
./lib/unused-1.0.0.jar,\
./lib/validation-api-1.1.0.Final.jar,\
./lib/xbean-asm5-shaded-4.4.jar,\
./lib/xmlenc-0.52.jar,\
./lib/xz-1.0.jar,\
./lib/zkclient-0.3.jar,\
./lib/zookeeper-3.4.5.jar"

CURR_TIME=`date +%Y%m%d%H%M`
LOG_FILE=zipper_${CURR_TIME}.log

echo "Please wait ..."
echo

hdfs dfs -rm -r /user/root/xiaoy/cp
hdfs dfs -mkdir -p /user/root/xiaoy/cp

/usr/hdp/current/spark2-client/bin/spark-submit \
	--class MySparkStreamingApp \
        --master yarn \
        --deploy-mode client \
        --num-executors 32 \
        --driver-memory 2g \
        --executor-memory 8g \
        --executor-cores 4 \
        --conf spark.scheduler.mode=FAIR \
        --conf spark.streaming.concurrentJobs=4 \
        --conf spark.default.parallelism=32 \
	--jars $DEPS \
	test-spark-2-1.0-SNAPSHOT.jar cluster master:6667,slave1:6667,slave2:6667,slave3:6667,slave4:6667,slave5:6667,slave6:6667,slave7:6667 tp1 30000 180 1 50000000 100000000 300000000 account-list


#grep Excep ${LOG_FILE} > /dev/null
#if [ $? -eq 1 ]; then
#	echo "Job finished successfully."
#	exit 0
#else
#	echo "Job failed with exception, please check ${LOG_FILE} for more details."
#	exit -1
#fi



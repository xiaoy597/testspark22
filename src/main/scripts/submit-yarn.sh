#!/bin/sh

SCHEMA_NAME=$1
TABLE_NAME=$2
LOAD_MONTH=$3
START_DATE=$4
END_DATE=$5
DEFAULT_END_DATE=$6

if [ -z "$SCHEMA_NAME" -o -z "$TABLE_NAME" -o -z "$LOAD_MONTH" -o -z "START_DATE" -o -z "$END_DATE" -o -z "$DEFAULT_END_DATE" ]; then
	echo "Insufficient program arguments."
	exit 1
fi

export ETL_METADB_SERVER=192.168.112.128:3306
export ETL_METADB_DBNAME=etl_metadata
export ETL_METADB_USER=root
export ETL_METADB_PASSWORD=root

export ETL_ZIPPER_DEBUG=true
export ETL_ZIPPER_PARTITION=4

DEPS="\
./lib/akka-actor_2.10-2.3.11.jar,\
./lib/akka-remote_2.10-2.3.11.jar,\
./lib/akka-slf4j_2.10-2.3.11.jar,\
./lib/antlr-2.7.7.jar,\
./lib/antlr-runtime-3.4.jar,\
./lib/apache-log4j-extras-1.2.17.jar,\
./lib/asm-3.1.jar,\
./lib/avro-1.7.7.jar,\
./lib/avro-ipc-1.7.7.jar,\
./lib/avro-ipc-1.7.7-tests.jar,\
./lib/avro-mapred-1.7.7-hadoop2.jar,\
./lib/bonecp-0.8.0.RELEASE.jar,\
./lib/calcite-avatica-1.2.0-incubating.jar,\
./lib/calcite-core-1.2.0-incubating.jar,\
./lib/calcite-linq4j-1.2.0-incubating.jar,\
./lib/chill_2.10-0.5.0.jar,\
./lib/chill-java-0.5.0.jar,\
./lib/commons-beanutils-1.7.0.jar,\
./lib/commons-beanutils-core-1.8.0.jar,\
./lib/commons-cli-1.2.jar,\
./lib/commons-codec-1.10.jar,\
./lib/commons-collections-3.2.1.jar,\
./lib/commons-compiler-2.7.6.jar,\
./lib/commons-compress-1.4.1.jar,\
./lib/commons-configuration-1.6.jar,\
./lib/commons-dbcp-1.4.jar,\
./lib/commons-digester-1.8.jar,\
./lib/commons-httpclient-3.1.jar,\
./lib/commons-io-2.4.jar,\
./lib/commons-lang-2.4.jar,\
./lib/commons-lang3-3.3.2.jar,\
./lib/commons-logging-1.1.3.jar,\
./lib/commons-math-2.1.jar,\
./lib/commons-math3-3.4.1.jar,\
./lib/commons-net-2.2.jar,\
./lib/commons-pool-1.5.4.jar,\
./lib/compress-lzf-1.0.3.jar,\
./lib/config-1.2.1.jar,\
./lib/curator-client-2.1.0-incubating.jar,\
./lib/curator-framework-2.4.0.jar,\
./lib/curator-recipes-2.4.0.jar,\
./lib/datanucleus-api-jdo-3.2.6.jar,\
./lib/datanucleus-core-3.2.10.jar,\
./lib/datanucleus-rdbms-3.2.9.jar,\
./lib/derby-10.10.2.0.jar,\
./lib/eigenbase-properties-1.1.5.jar,\
./lib/groovy-all-2.1.6.jar,\
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
./lib/hive-exec-1.2.1.spark.jar,\
./lib/hive-metastore-1.2.1.spark.jar,\
./lib/httpclient-4.3.2.jar,\
./lib/httpcore-4.3.1.jar,\
./lib/ivy-2.4.0.jar,\
./lib/jackson-annotations-2.6.0.jar,\
./lib/jackson-core-2.6.3.jar,\
./lib/jackson-core-asl-1.9.13.jar,\
./lib/jackson-databind-2.6.3.jar,\
./lib/jackson-mapper-asl-1.9.13.jar,\
./lib/jackson-module-scala_2.10-2.4.4.jar,\
./lib/janino-2.7.8.jar,\
./lib/JavaEWAH-0.3.2.jar,\
./lib/javax.servlet-3.0.0.v201112011016.jar,\
./lib/javolution-5.5.1.jar,\
./lib/jcl-over-slf4j-1.7.10.jar,\
./lib/jdo-api-3.0.1.jar,\
./lib/jersey-core-1.9.jar,\
./lib/jersey-server-1.9.jar,\
./lib/jets3t-0.7.1.jar,\
./lib/jetty-util-6.1.26.jar,\
./lib/jline-2.12.jar,\
./lib/joda-time-2.5.jar,\
./lib/jodd-core-3.5.2.jar,\
./lib/json-20090211.jar,\
./lib/json4s-ast_2.10-3.2.10.jar,\
./lib/json4s-core_2.10-3.2.10.jar,\
./lib/json4s-jackson_2.10-3.2.10.jar,\
./lib/jsoup-1.8.3.jar,\
./lib/jsr305-1.3.9.jar,\
./lib/jta-1.1.jar,\
./lib/jul-to-slf4j-1.7.10.jar,\
./lib/kryo-2.21.jar,\
./lib/libfb303-0.9.2.jar,\
./lib/libthrift-0.9.2.jar,\
./lib/log4j-1.2.17.jar,\
./lib/log4j-api-2.4.1.jar,\
./lib/log4j-core-2.4.1.jar,\
./lib/log4j-slf4j-impl-2.4.1.jar,\
./lib/lz4-1.3.0.jar,\
./lib/mesos-0.21.1-shaded-protobuf.jar,\
./lib/metrics-core-3.1.2.jar,\
./lib/metrics-graphite-3.1.2.jar,\
./lib/metrics-json-3.1.2.jar,\
./lib/metrics-jvm-3.1.2.jar,\
./lib/minlog-1.2.jar,\
./lib/mysql-connector-java-5.1.18.jar,\
./lib/netty-3.8.0.Final.jar,\
./lib/netty-all-4.0.29.Final.jar,\
./lib/objenesis-1.2.jar,\
./lib/opencsv-2.3.jar,\
./lib/oro-2.0.8.jar,\
./lib/paranamer-2.6.jar,\
./lib/parquet-column-1.7.0.jar,\
./lib/parquet-common-1.7.0.jar,\
./lib/parquet-encoding-1.7.0.jar,\
./lib/parquet-format-2.3.0-incubating.jar,\
./lib/parquet-generator-1.7.0.jar,\
./lib/parquet-hadoop-1.7.0.jar,\
./lib/parquet-hadoop-bundle-1.6.0.jar,\
./lib/parquet-jackson-1.7.0.jar,\
./lib/protobuf-java-2.5.0.jar,\
./lib/py4j-0.8.2.1.jar,\
./lib/pyrolite-4.4.jar,\
./lib/reflectasm-1.07-shaded.jar,\
./lib/RoaringBitmap-0.4.5.jar,\
./lib/scala-compiler-2.10.0.jar,\
./lib/scala-library-2.10.5.jar,\
./lib/scalap-2.10.0.jar,\
./lib/scala-reflect-2.10.4.jar,\
./lib/slf4j-api-1.7.12.jar,\
./lib/slf4j-log4j12-1.7.10.jar,\
./lib/snappy-0.2.jar,\
./lib/snappy-java-1.1.1.7.jar,\
./lib/spark-catalyst_2.10-1.5.0.jar,\
./lib/spark-core_2.10-1.5.0.jar,\
./lib/spark-hive_2.10-1.5.0.jar,\
./lib/spark-launcher_2.10-1.5.0.jar,\
./lib/spark-network-common_2.10-1.5.0.jar,\
./lib/spark-network-shuffle_2.10-1.5.0.jar,\
./lib/spark-sql_2.10-1.5.0.jar,\
./lib/spark-unsafe_2.10-1.5.0.jar,\
./lib/ST4-4.0.4.jar,\
./lib/stax-api-1.0.1.jar,\
./lib/stream-2.7.0.jar,\
./lib/stringtemplate-3.2.1.jar,\
./lib/tachyon-client-0.7.1.jar,\
./lib/tachyon-underfs-hdfs-0.7.1.jar,\
./lib/tachyon-underfs-local-0.7.1.jar,\
./lib/uncommons-maths-1.2.2a.jar,\
./lib/unused-1.0.0.jar,\
./lib/xmlenc-0.52.jar,\
./lib/xz-1.0.jar,\
./lib/zookeeper-3.4.5.jar"

CURR_TIME=`date +%Y%m%d%H%M`
LOG_FILE=zipper_${CURR_TIME}.log

echo "Please wait ..."
echo

spark-submit \
	--class io.jacob.etlman.processor.Zipper \
	--master yarn \
	--num-executors 1 \
	--driver-memory 512M \
	--executor-memory 256M \
	--executor-cores 2 \
	--conf spark.default.parallelism=$ETL_ZIPPER_PARTITION \
	--jars $DEPS \
	etlman-1.0-SNAPSHOT.jar cluster $SCHEMA_NAME $TABLE_NAME $LOAD_MONTH $START_DATE $END_DATE $DEFAULT_END_DATE 1>${LOG_FILE} 2>&1


grep Excep ${LOG_FILE} > /dev/null
if [ $? -eq 1 ]; then
	echo "Job finished successfully."
	exit 0
else
	echo "Job failed with exception, please check ${LOG_FILE} for more details."
	exit -1
fi



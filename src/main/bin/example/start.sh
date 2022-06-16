#!/bin/bash
P=`dirname $0`
DIR_NAME=${P##*/}
CURRENT_DIR=$(cd $P;pwd)
MASTER_DIR=${CURRENT_DIR%/*}
BASE_DIR=${MASTER_DIR%/*}
CONF_DIR=$BASE_DIR/conf
LIB_DIR=$BASE_DIR/lib
LOG_DIR=$BASE_DIR/log

version=1.5
master=yarn-cluster
name=himma-$DIR_NAME
jars=$LIB_DIR/spark-util_2.10-$version.jar,/opt/cloudera/parcels/CDH/jars/hbase-spark-1.2.0-cdh5.10.0.jar
packages=com.alibaba.otter:canal.client:1.0.23,com.alibaba.otter:canal.common:1.0.23,com.alibaba.otter:canal.protocol:1.0.23,mysql:mysql-connector-java:5.1.40,org.apache.commons:commons-email:1.4,org.scalikejdbc:scalikejdbc_2.10:2.5.0,com.mchange:c3p0:0.9.5.2
diver_class_path=/opt/cm-5.10.0/share/cmf/lib/mysql-connector-java-5.1.40.jar
class=com.qbao.spark.job.himma.Launcher

executable_jar=$LIB_DIR/spark-himma_2.10-$version.jar

spark-submit \
        --master $master \
        --name $name \
        --jars $jars \
        --packages $packages \
        --driver-class-path $diver_class_path \
        --class $class \
        --driver-memory 2G \
        --executor-memory 2G \
        --conf spark.kryoserializer.buffer=128m \
        --conf spark.kryoserializer.buffer.max=512m \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.network.timeout=12000s \
        --conf spark.executor.heartbeatInterval=1200s \
        --conf spark.default.parallelism=20 \
        --conf spark.storage.memoryFraction=0.6 \
        --conf spark.shuffle.memoryFraction=0.3 \
        --files $CONF_DIR/log4j.properties,$CONF_DIR/$DIR_NAME/application.properties \
$executable_jar $*
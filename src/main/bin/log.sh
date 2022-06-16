#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
BASE_DIR=${CURRENT_DIR%/*}
CONF_DIR=$BASE_DIR/conf
LIB_DIR=$BASE_DIR/lib
LOG_DIR=$BASE_DIR/log
if [ -z "$1" -o -z "$2" ]
then
        echo -e "\033[40;32;1mUsage: $0 <himma-instance-name> [log length]\033[0m"
        echo -e "\033[40;31;1mPlease use correct parameters\033[0m"
        exit 1
fi
if [ ! -d $CURRENT_DIR/$1 ]
then
        echo -e "\033[40;31;1mInvalid himma-instance-name\033[0m"
        exit 1
fi
id=`yarn application -list | grep himma-$1 | awk '{printf $1}'`
yrm=cdh42:8088
log_url=`curl $yrm/ws/v1/cluster/apps/$id | python -m json.tool | grep -Po '"amContainerLogs": ".*?"' | grep -Po 'http://[^"]*'`
redirect=`curl $log_url/stderr/?start=$2 | grep Redirecting`
if [[ $redirect != ""  ]]
then
        r_url=${log##* /}
        curl $r_url
else
        curl $log_url/stderr/?start=$2
fi
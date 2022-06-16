#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
BASE_DIR=${CURRENT_DIR%/*}
CONF_DIR=$BASE_DIR/conf
LIB_DIR=$BASE_DIR/lib
LOG_DIR=$BASE_DIR/log
if [ -z "$1" ]
then
        echo -e "\033[40;32;1mUsage: $0 <actived profiles> [jar parameters]\033[0m"
        echo -e "\033[40;31;1mPlease use correct parameters\033[0m"
        exit 1
fi
if [ ! -d $CURRENT_DIR/$1 ]
then
        echo -e "\033[40;31;1mInvalid himma-instance-name\033[0m"
        exit 1
fi
num=`yarn application -list | grep himma-$1 | wc -l`
if [ $num -eq 1 ]
then
        echo -e "\033[40;31;1mHimma application already started, wanna restart it? Kill it first.\033[0m"
        exit 1
fi
nohup $BASE_DIR/bin/$1/start.sh $* > /dev/null 2>&1 &
echo -e "\033[40;32;1mHimma application $1 started with parameters $* and with pid $!\033[0m"
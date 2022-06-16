#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
if [ -z "$1" ]
then
        echo -e "\033[40;32;1mUsage: $0 himma-instance-name\033[0m"
        echo -e "\033[40;31;1mPlease use correct parameters\033[0m"
        exit 1
fi
if [ ! -d $CURRENT_DIR/$1 ]
then
        echo -e "\033[40;31;1mInvalid himma-instance-name\033[0m"
        exit 1
fi
echo -e "\033[40;32;1mGet status of application $1, current dir is: $CURRENT_DIR\033[0m"
allnum=1
deadnum=0
name=himma-$1
num=`yarn application -list | grep "$name" | wc -l`
if [ "$num" = 0 ]
then
        let deadnum+=1
        printf "\033[40;31;1mHimma application\033[0m"
        printf " $1 "
        echo -e "\033[40;31;1mis dead\033[0m"
else
        printf "\033[40;32;1mHimma application\033[0m"
        printf " $1 "
        echo -e "\033[40;32;1mis running\033[0m"
fi
printf "\033[40;31;1m$deadnum/$allnum \033[0m"
echo -e "\033[40;32;1mhimma applications are dead\033[0m"
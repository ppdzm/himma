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
        echo -e "\033[40;31;1mInvalid himma instance name\033[0m"
        exit 1
fi
echo -e "\033[40;31;1mYou are try to stop himma applicaiton himma-$1, please input y/Y(other to abort) to confirm your action!!!!\033[0m"
printf  "\033[40;32;1mPlease input:\033[0m"
read input
if [ $input == "y" -o $input == "Y" ]
then
        if [ -d $CURRENT_DIR/$1 ]
        then
                id=`yarn application -list | grep application | grep himma-$1 | awk '{ print $1 }'`
                if [ ! -z $id ]
                then
                        echo -e "\033[40;32;1mStop himma application $1($id) now\033[0m"
                        r=`yarn application -kill $id`
                else
                        printf "\033[40;31;1mCan not find himma application\033[0m"
                        printf " $1 "
                        echo -e "\033[40;31;1mmaybe it was dead/stopped already\033[0m"
                fi
        fi
fi
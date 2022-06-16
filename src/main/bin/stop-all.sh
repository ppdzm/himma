#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
echo -e "\033[40;31;1mYou are try to stop all himma applicaitons, please input y/Y(other to abort) to confirm your action!!!!\033[0m"
printf  "\033[40;32;1mPlease input:\033[0m"
read input
app_list=`yarn application -list`
if [ $input == "y" -o $input == "Y" ]
then
        for e in `ls $CURRENT_DIR`
        do
                if [ -d $CURRENT_DIR/$e ]
                then
                        num=`echo $app_list | grep $e | wc -l`
                        if [ $num -ne 0 ]
                        then
                                id=`yarn application -list | grep application | grep himma-$e | awk '{ print $1 }'`
                                echo -e "\033[40;32;1mStop himma application $e($id) now\033[0m"
                                yarn application -kill $id
                        else
                                printf "\033[40;31;1mCan not find himma application\033[0m"
                                printf " $e "
                                echo -e "\033[40;31;1mmaybe it was dead/stopped already\033[0m"
                        fi
                fi
        done
fi
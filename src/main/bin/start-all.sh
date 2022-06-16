#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
BASE_DIR=${CURRENT_DIR%/*}
dir_pattern="[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[a-zA-Z0-9]+"
app_list=`yarn application -list`
for e in `ls $CURRENT_DIR`
do
        if [ -d $CURRENT_DIR/$e ] && [[ "$e" =~ $dir_pattern ]]
        then
                name=himma-$e
                num=`echo $app_list | grep "$name" | wc -l`
                if [ "$num" = 0 ]
                then
                        $BASE_DIR/bin/start.sh $e $*
                else
                        printf "\033[40;31;1mHimma application\033[0m"
                        printf " $e "
                        echo -e "\033[40;31;1malready started, wanna restart it? Kill it first.\033[0m"
                fi
        fi
done
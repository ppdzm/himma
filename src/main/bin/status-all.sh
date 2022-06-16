#!/bin/bash
DIR_NAME=`dirname $0`
CURRENT_DIR=$(cd $DIR_NAME;pwd)
echo -e "\033[40;32;1mGet status of all applications, current dir is: $CURRENT_DIR\033[0m"
allnum=0
deadnum=0
dir_pattern="[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[a-zA-Z0-9]+"
app_list=`yarn application -list`
for e in `ls $CURRENT_DIR`
do
        if [ -d $CURRENT_DIR/$e ] && [[ "$e" =~ $dir_pattern ]]
        then
                let allnum+=1
                name=himma-$e
                num=`echo $app_list | grep "$name" | wc -l`
                if [ "$num" = 0 ]
                then
                        let deadnum+=1
                        printf "\033[40;31;1mhimma application\033[0m"
                        printf " $e "
                        echo -e "\033[40;31;1mis dead\033[0m"
                else
                        printf "\033[40;32;1mhimma application\033[0m"
                        printf " $e "
                        echo -e "\033[40;32;1mis running\033[0m"
                fi
        fi
done
printf "\033[40;31;1m$deadnum/$allnum \033[0m"
echo -e "\033[40;32;1mhimma applications are dead\033[0m"
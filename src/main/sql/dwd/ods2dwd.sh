#!/bin/bash
source /etc/profile
args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 day ago" "+%Y%m%d"`
else
dt=$1
fi
echo "locd dwd hive.starting...."
hive -hivevar date_dt=${dt} -f /home/dwdloaddata.hql
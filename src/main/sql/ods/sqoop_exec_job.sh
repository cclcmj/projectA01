#!/bin/bash
##########################
#执行sqoop的job（包括增量全量两种任务）
#输入的日期参数将作为增量导入分区表的分区值
#run with args.eg: ./load_data.sh 2018-12-25
##########################
##默认参数为当前日期的前一天
args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 days ago" "+%Y%m%d"`
else
dt=$1
fi
echo $dt
source /etc/profile
##运行sqoop的job
#全量导入job
echo "load full data job by sqoop.starting...."
sqoop job -exec bap_code_category
sqoop job -exec bap_user 
sqoop job -exec bap_user_extend
sqoop job -exec bap_user_addr
echo "load full data job by sqoop.ended...."
#增量导入job
echo "load incr data job by sqoop.starting...."
sqoop job -exec bap_us_order
sqoop job -exec bap_cart
sqoop job -exec bap_order_delivery
sqoop job -exec bap_order_item
sqoop job -exec bap_user_app_click_log
sqoop job -exec bap_user_pc_click_log
echo "load incr data job by sqoop.ended...."
##将数据导入hive表中
#非分区表（全量导入）
echo "run hive load full data.starting...."
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_code_category/*' into table qfbap_ods.ods_code_category"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user/*' into table qfbap_ods.ods_user"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_extend/*' into table qfbap_ods.ods_user_extend"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_addr/*' into table qfbap_ods.ods_user_addr"
echo "run hive load full data.ended...."
#分区表（增量导入）
echo "run hive load incr(partition) data.starting...."
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_us_order/*' into table qfbap_ods.ods_us_order partition(dt=${dt})"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_cart/*' into table qfbap_ods.ods_cart partition(dt=${dt})"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_order_delivery/*' into table qfbap_ods.ods_order_delivery partition(dt=${dt})"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_order_item/*' into table qfbap_ods.ods_order_item partition(dt=${dt})"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_app_click_log/*' into table qfbap_ods.ods_user_app_click_log partition(dt=${dt})"
hive --database qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_pc_click_log/*' into table qfbap_ods.ods_user_pc_click_log partition(dt=${dt})"
echo "run hive load incr(partition) data.ended...."

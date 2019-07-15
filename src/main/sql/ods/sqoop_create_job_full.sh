#!/bin/bash
######################################
##创建sqoop的job（全量导入）
######################################
#---->bap_code_category
sqoop job --create bap_code_category -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table code_category \
--delete-target-dir \
--target-dir /qfbap/ods/ods_tmp/ods_code_category \
--fields-terminated-by '\001'
#---->bap_user
sqoop job --create bap_user -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user \
--delete-target-dir \
--target-dir /qfbap/ods/ods_tmp/ods_user \
--fields-terminated-by '\001'
#---->bap_user_extend
sqoop job --create bap_user_extend -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_extend \
--delete-target-dir \
--target-dir /qfbap/ods/ods_tmp/ods_user_extend \
--fields-terminated-by '\001'
#---->bap_user_addr
sqoop job --create bap_user_addr -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_addr
--delete-target-dir \
--target-dir /qfbap/ods/ods_tmp/ods_user_addr \
--fields-terminated-by '\001'

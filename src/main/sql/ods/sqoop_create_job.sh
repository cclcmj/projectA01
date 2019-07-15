#!/bin/bash
#########################################
##创建sqoop的job（全量导入和增量导入）
#########################################
##全量导入
source /etc/profile
#---->bap_code_category
sqoop job --delete bap_code_category
sqoop job --create bap_code_category -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table code_category \
--delete-target-dir \
--target-dir /qfbap/ods_tmp/ods_code_category \
--fields-terminated-by '\001'
#---->bap_user
sqoop job --delete bap_user
sqoop job --create bap_user -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user \
--delete-target-dir \
--target-dir /qfbap/ods_tmp/ods_user \
--fields-terminated-by '\001'
#---->bap_user_extend
sqoop job --delete bap_user_extend
sqoop job --create bap_user_extend -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_extend \
--delete-target-dir \
--target-dir /qfbap/ods_tmp/ods_user_extend \
--fields-terminated-by '\001'
#---->bap_user_addr
sqoop job --delete bap_user_addr
sqoop job --create bap_user_addr -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_addr \
--delete-target-dir \
--target-dir /qfbap/ods_tmp/ods_user_addr \
--fields-terminated-by '\001'
##增量导入
#---->bap_us_order
sqoop job --delete bap_us_order
sqoop job --create bap_us_order -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table us_order \
--target-dir /qfbap/ods_tmp/ods_us_order \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0
#---->bap_cart
sqoop job --delete bap_cart
sqoop job --create bap_cart -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table cart \
--target-dir /qfbap/ods_tmp/ods_cart \
--fields-terminated-by '\001' \
--check-column cart_id \
--incremental append \
--last-value 0
#---->bap_order_delivery
sqoop job --delete bap_order_delivery
sqoop job --create bap_order_delivery -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table order_delivery \
--target-dir /qfbap/ods_tmp/ods_order_delivery \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0
#---->bap_order_item
sqoop job --delete bap_order_item
sqoop job --create bap_order_item -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table order_item \
--target-dir /qfbap/ods_tmp/ods_order_item \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0
#---->bap_user_app_click_log
sqoop job --delete bap_user_app_click_log
sqoop job --create bap_user_app_click_log -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_app_click_log \
--target-dir /qfbap/ods_tmp/ods_user_app_click_log \
--fields-terminated-by '\001' \
--check-column log_id \
--incremental append \
--last-value 0
#---->bap_user_pc_click_log
sqoop job --delete bap_user_pc_click_log
sqoop job --create bap_user_pc_click_log -- import \
--connect jdbc:mysql://node4:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password 123456 \
--table user_pc_click_log \
--target-dir /qfbap/ods_tmp/ods_user_pc_click_log \
--fields-terminated-by '\001' \
--check-column log_id \
--incremental append \
--last-value 0

INSERT OVERWRITE TABLE qfbap_dwd.dwd_biz_trade partition (dt=${hivevar:date_dt})
SELECT
trade_id,order_id,user_id,amount,trade_type,trade_time,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_biz_trade;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_cart partition (dt=${hivevar:date_dt})
SELECT
cart_id,session_id,user_id,goods_id,goods_num,add_time,cancle_time,sumbit_time,create_date,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_cart;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_code_category
SELECT
first_category_id,first_category_name,second_category_id,second_catery_name,third_category_id,third_category_name,category_id,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_code_category;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_order_delivery partition (dt=${hivevar:date_dt})
SELECT
order_id,order_no,consignee,area_id,area_name,address,mobile,phone,coupon_id,coupon_money,carriage_money,create_time,update_time,addr_id,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_order_delivery;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_order_item partition (dt=${hivevar:date_dt})
SELECT
user_id,order_id,order_no,goods_id,goods_no,goods_name,goods_amount,shop_id,shop_name,curr_price,market_price,discount,cost_price,first_cart,first_cart_name,second_cart,second_cart_name,third_cart,third_cart_name,goods_desc,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_order_item;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_us_order partition (dt=${hivevar:date_dt})
SELECT
order_id,order_no,order_date,user_id,user_name,order_money,order_type,order_status,pay_status,pay_type,order_source,update_time,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_us_order;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_user
SELECT
user_id,user_name,user_gender,user_birthday,user_age,constellation,province,city,city_level,e_mail,op_mail,mobile,num_seg_mobile,op_Mobile,register_time,login_ip,login_source,request_user,total_score,used_score,is_blacklist,is_married,education,monthly_income,profession,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_user;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_user_addr
SELECT
user_id,order_addr,user_order_flag,addr_id,arear_id,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_user_addr;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_user_extend
SELECT
user_id,user_gender,is_pregnant_woman,is_have_children,is_have_car,phone_brand,phone_brand_level,phone_cnt,change_phone_cnt,is_maja,majia_account_cnt,loyal_model,shopping_type_model,weight,height,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_user_extend;
INSERT  OVERWRITE TABLE  qfbap_dwd.dwd_user_pc_pv partition (dt=${hivevar:date_dt})
SELECT
max(log_id),
user_id,session_id,cookie_id,
min(to_date(visit_time)),
max(to_date(visit_time)),
case when max(to_date(visit_time))=min(to_date(visit_time)) then 3 else max(to_date(visit_time))-min(to_date(visit_time)) end,
count(1),
visit_os,browser_name,visit_ip,province,city,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_user_pc_click_log
WHERE 
dt=dt=${hivevar:date_dt};
GROUP By
user_id,session_id,cookie_id,visit_os,browser_name,visit_ip,province,city;
INSERT OVERWRITE TABLE qfbap_dwd.dwd_user_app_pv partition (dt=${hivevar:date_dt})
SELECT
log_id,user_id,imei,log_time,hour(log_time),visit_os,os_version,app_name,app_version,device_token,visit_ip,province,city,CURRENT_TIMESTAMP
FROM
qfbap_ods.ods_user_app_click_log
WHERE
dt=dt=${hivevar:date_dt};
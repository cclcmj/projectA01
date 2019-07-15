SELECT
u.user_id,
u.user_name,
u.user_gender,
u.user_birthday,
u.user_age,
u.constellation,
u.province,
u.city,
u.city_level,
u.e_mail,
u.op_mail,
u.mobile,
u.num_seg_mobile,
u.op_Mobile,
u.register_time,
u.login_ip,
u.login_source,
u.request_user,
u.total_score,
u.used_score,
u.is_blacklist,
u.is_married,
u.education,
u.monthly_income,
u.profession,
e.is_pregnant_woman,
e.is_have_children,
e.is_have_car,
e.phone_brand,
e.phone_brand_level,
e.phone_cnt,
e.change_phone_cnt,
e.is_maja,
e.majia_account_cnt,
e.loyal_model,
e.shopping_type_model,
e.weight,
e.height
FROM
qfbap_dwd.dwd_user AS u
JOIN
qfbap_dwd.dwd_user_extend AS e
ON
u.user_id=e.user_id;



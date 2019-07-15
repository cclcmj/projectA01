select 
pc_last_visit_time,
pc_last_visit_session_id,
pc_last_visit_pv,
pc_last_visit_browser_name,
pc_last_visit_os,
pc_first_visit_time,
pc_first_visit_session_id,
pc_first_visit_cookie_id,
pc_first_visit_pv,
pc_first_visit_browser_name,
pc_first_visit_os,
pc_cnt_dt7,
pc_cnt_dt15,
pc_cnt_dt30,
pc_cnt_dt60,
pc_cnt_dt90,
pc_day30_pc_visit_time,
pc_day30_pc_pv,
pc_day30_pc_avg_pv,
pc_day30_pc_hour025_cnt,
pc_day30_pc_hour627_cnt,
pc_day30_pc_hour829_cnt,
pc_day30_pc_hour10211_cnt,
pc_day30_pc_hour12213_cnt,
pc_day30_pc_hour14216_cnt,
pc_day30_pc_hour17219_cnt,
pc_day30_pc_hour18219_cnt,
pc_day30_pc_hour20221_cnt,
pc_day30_pc_hour22223_cnt,
pc_day30_pc_diff_ip_cnt,
pc_day30_maxnu_ip,
pc_day_pc_cookie_id_cnt,
pc_day30_maxnu_browser_name,
pc_day30_maxnu_visit_os,
app_last_app_visit_time,
app_first_app_name,
app_first_app_visit_os,
app_first_app_visit_ip,
app_app_dt7_cnt,
app_app_dt15_cnt,
app_app_dt30_cnt, 
app_app_dt60_cnt,
app_app_dt90_cnt
from(


select 

t2.last_visit_time pc_last_visit_time,
t2.last_visit_session_id pc_last_visit_session_id,
t2.last_visit_pv pc_last_visit_pv,
t2.last_visit_browser_name pc_last_visit_browser_name,
t2.last_visit_os pc_last_visit_os,
t2.first_visit_time pc_first_visit_time,
t2.first_visit_session_id pc_first_visit_session_id,
t2.first_visit_cookie_id pc_first_visit_cookie_id,
t2.first_visit_pv pc_first_visit_pv,
t2.first_visit_browser_name pc_first_visit_browser_name,
t2.first_visit_os pc_first_visit_os,
cnt_dt7 pc_cnt_dt7,
cnt_dt15 pc_cnt_dt15,
cnt_dt30 pc_cnt_dt30,
cnt_dt60 pc_cnt_dt60,
cnt_dt90 pc_cnt_dt90,
t2.day30_pc_visit_time pc_day30_pc_visit_time,
t2.day30_pc_pv pc_day30_pc_pv,
t2.day30_pc_avg_pv pc_day30_pc_avg_pv,
t2.day30_pc_hour025_cnt pc_day30_pc_hour025_cnt,
t2.day30_pc_hour627_cnt pc_day30_pc_hour627_cnt,
t2.day30_pc_hour829_cnt pc_day30_pc_hour829_cnt,
t2.day30_pc_hour10211_cnt pc_day30_pc_hour10211_cnt,
t2.day30_pc_hour12213_cnt pc_day30_pc_hour12213_cnt,
t2.day30_pc_hour14216_cnt pc_day30_pc_hour14216_cnt,
t2.day30_pc_hour17219_cnt pc_day30_pc_hour17219_cnt,
t2.day30_pc_hour18219_cnt pc_day30_pc_hour18219_cnt,
t2.day30_pc_hour20221_cnt pc_day30_pc_hour20221_cnt,
t2.day30_pc_hour22223_cnt pc_day30_pc_hour22223_cnt,
t3.day30_pc_diff_ip_cnt pc_day30_pc_diff_ip_cnt,
t3.day30_maxnu_ip pc_day30_maxnu_ip,
day_pc_cookie_id_cnt pc_day_pc_cookie_id_cnt,
day30_maxnu_cookie_id pc_day30_maxnu_cookie_id,
day30_maxnu_browser_name pc_day30_maxnu_browser_name,
day30_maxnu_visit_os pc_day30_maxnu_visit_os,
app_t2.last_app_visit_time app_last_app_visit_time,
app_t2.last_app_name app_last_app_name,
app_t2.last_app_visit_os app_last_app_visit_os,
app_t2.first_app_visit_time app_first_app_visit_time,
app_t2.first_app_name app_first_app_name,
app_t2.first_app_visit_os app_first_app_visit_os,
app_t2.first_app_visit_ip app_first_app_visit_ip,
app_t2.app_dt7_cnt app_app_dt7_cnt,
app_t2.app_dt15_cnt app_app_dt15_cnt,
app_t2.app_dt30_cnt app_app_dt30_cnt, 
app_t2.app_dt30_cnt app_app_dt60_cnt, 
app_t2.app_dt90_cnt app_app_dt90_cnt



from
qfbap_dm.dm_user_basic
left join
(
select
user_id,
max(case when t1.rn_desc=1 then from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') end) last_visit_time,
max(case when t1.rn_desc=1 then session_id end) last_visit_session_id,
max(case when t1.rn_desc=1 then cookie_id end) last_visit_cookie_id,
max(case when t1.rn_desc=1 then pv end) last_visit_pv,
max(case when t1.rn_desc=1 then browser_name end) last_visit_browser_name,
max(case when t1.rn_desc=1 then visit_os end) last_visit_os,
max(case when t1.rn_asc=1 then from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') end )
first_visit_time,
max(case when t1.rn_asc=1 then session_id end) first_visit_session_id,
max(case when t1.rn_asc=1 then cookie_id end) first_visit_cookie_id,
max(case when t1.rn_asc=1 then pv end) first_visit_pv,
max(case when t1.rn_asc=1 then browser_name end) first_visit_browser_name,
max(case when t1.rn_asc=1 then visit_os end) first_visit_os,
sum(dt7) cnt_dt7,
sum(dt15) cnt_dt15,
sum(dt30) cnt_dt30,
sum(dt60) cnt_dt60,
sum(dt90) cnt_dt90,
count(distinct(case when dt30=1 then from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') end)) day30_pc_visit_time,
sum(case when dt30=1 then pv end) day30_pc_pv, 
sum(case when dt30=1 then pv end)/count(distinct(case when dt30=1 then  from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') end)) day30_pc_avg_pv,
count(case when dt30=1 and hr025 =1 then 1 end) day30_pc_hour025_cnt,
count(case when dt30=1 and hr627=1 then 1 end) day30_pc_hour627_cnt,
count(case when dt30=1 and hr829=1 then 1 end) day30_pc_hour829_cnt,
count(case when dt30=1 and hr10211=1 then 1 end) day30_pc_hour10211_cnt,
count(case when dt30=1 and hr12213=1 then 1 end) day30_pc_hour12213_cnt,
count(case when dt30=1 and hr14216=1 then 1 end) day30_pc_hour14216_cnt,
count(case when dt30=1 and hr17219=1 then 1 end) day30_pc_hour17219_cnt,
count(case when dt30=1 and hr18219=1 then 1 end ) day30_pc_hour18219_cnt,
count(case when dt30=1 and hr20221=1 then 1 end) day30_pc_hour20221_cnt,
count(case when dt30=1 and hr22223=1 then 1 end) day30_pc_hour22223_cnt
from(
select 
row_number() over (distribute by user_id sort by in_time) rn_asc,
row_number() over (distribute by user_id sort by in_time desc) rn_desc,
user_id,
session_id,
cookie_id,
pv,
in_time,
browser_name,
visit_os,
visit_ip,
(case when from_unixtime(cast(in_time as bigint),'yyyy-MM-dd')>= date_add("2019-07-12",-6) then 1 end) dt7,
(case when from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') >= date_add("2019-07-12",-14) then 1 end) dt15,
(case when from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') >=date_add("2019-07-12",-29) then 1 end) dt30,
(case when from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') >=date_add("2019-07-12",-59) then 1 end ) dt60,
(case when from_unixtime(cast(in_time as bigint),'yyyy-MM-dd') >=date_add("2019-07-12",-89) then 1 end) dt90,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss'))  between 0 and 5 then 1 end) hr025,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between  6 and 7 then 1 end) hr627,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 8 and 9 then 1 end) hr829,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 10 and 11 then 1 end) hr10211,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 12 and 13 then 1 end) hr12213,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 14 and 16 then 1 end) hr14216,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 17 and 19 then 1 end) hr17219,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 18 and 19 then 1 end) hr18219,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 20 and 21 then 1 end) hr20221,
(case when hour(from_unixtime(cast(in_time as bigint),'yyyy-MM-dd HH:mm:ss')) between 22 and 23 then 1 end) hr22223
from 
qfbap_dwd.dwd_user_pc_pv
) t1

group by user_id
) t2
on
t2.user_id=dm_user_basic.user_id
left join
(
select
user_id,
count(distinct(case when type='visit_ip' then content end)) day30_pc_diff_ip_cnt,
max(case when rn=1 and type='visit_ip' then content end) day30_maxnu_ip,
count(case when type='cookie_id' then content end) day_pc_cookie_id_cnt,
max(case when rn=1 and type='cookie_id' then content end) day30_maxnu_cookie_id,
max(case when rn=1 and type='browser_name' then content end) day30_maxnu_browser_name,
max(case when rn=1 and type='visit_os' then content end) day30_maxnu_visit_os
from 
qfbap_dws.dws_user_visit_month1
group by user_id
) t3
on 
t3.user_id=dm_user_basic.user_id
left join
(
select
user_id,
max(case when app_t1.app_desc=1 then log_time end) last_app_visit_time,
max(case when app_t1.app_desc=1 then app_name end) last_app_name,
max(case when app_t1.app_desc=1 then visit_os end) last_app_visit_os,
max(case when app_t1.app_asc=1 then log_time end) first_app_visit_time,
max(case when app_t1.app_asc=1 then app_name end) first_app_name,
max(case when app_t1.app_asc=1 then visit_os end) first_app_visit_os,
max(case when app_t1.app_asc=1 then visit_ip end) first_app_visit_ip,
sum(dt7) app_dt7_cnt,
sum(dt15) app_dt15_cnt,
sum(dt30) app_dt30_cnt,
sum(dt60) app_dt60_cnt,
sum(dt90) app_dt90_cnt,

count(case when app_t1.dt30=1 and app_hr025 =1 then 1 end) day30_app_hour025_cnt,
count(case when app_t1.dt30=1 and app_hr627=1 then 1 end) day30_app_hour627_cnt,
count(case when app_t1.dt30=1 and app_hr829=1 then 1 end) day30_app_hour829_cnt,
count(case when app_t1.dt30=1 and app_hr10211=1 then 1 end) day30_app_hour10211_cnt,
count(case when app_t1.dt30=1 and app_hr12213=1 then 1 end) day30_app_hour12213_cnt,
count(case when app_t1.dt30=1 and app_hr14215=1 then 1 end) day30_app_hour14215_cnt,
count(case when app_t1.dt30=1 and app_hr16217=1 then 1 end) day30_app_hour16217_cnt,
count(case when app_t1.dt30=1 and app_hr18219=1 then 1 end) day30_app_hour18219_cnt,
count(case when app_t1.dt30=1 and app_hr20221=1 then 1 end) day30_app_hour20221_cnt,
count(case when app_t1.dt30=1 and app_hr22223=1 then 1 end) day30_app_hour22223_cnt
from
(
select 
row_number() over(distribute by user_id sort by log_time) app_asc,
row_number() over(distribute by user_id sort by log_time desc) app_desc,
log_time,
user_id,
app_name,
app_version,
visit_os,
visit_ip,
pv,
(case when log_time>= date_add("2019-07-12",-6) then 1 else 0 end) dt7,
(case when log_time>= date_add("2019-07-12",-14) then 1 else 0 end) dt15,
(case when log_time>= date_add("2019-07-12",-29) then 1 else 0 end) dt30,
(case when log_time>= date_add("2019-07-12",-59) then 1 else 0 end) dt60,
(case when log_time>= date_add("2019-07-12",-89) then 1 else 0 end) dt90,
(case when log_hour between 0 and 5 then 1 else 0 end) app_hr025,
(case when log_hour between 6 and 7 then 1 else 0 end) app_hr627,
(case when log_hour between 8 and 9 then 1 else 0 end) app_hr829,
(case when log_hour between 10 and 11 then 1 else 0 end) app_hr10211,
(case when log_hour between 12 and 13 then 1 else 0 end) app_hr12213,
(case when log_hour between 14 and 15 then 1 else 0 end) app_hr14215,
(case when log_hour between 16 and 17 then 1 else 0 end) app_hr16217,
(case when log_hour between 18 and 19 then 1 else 0 end) app_hr18219,
(case when log_hour between 20 and 21 then 1 else 0 end) app_hr20221,
(case when log_hour between 22 and 23 then 1 else 0 end) app_hr22223
from
qfbap_dwd.dwd_user_app_pv
) app_t1
group by user_id
) app_t2
on
app_t2.user_id=dm_user_basic.user_id
) comtable
;
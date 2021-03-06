SELECT
tmp2.user_id,
tmp2.type,
tmp2.cnt,
tmp2.content,
tmp2.rn,
tmp2.dw_date
FROM
(SELECT
tmp.user_id,
tmp.type,
tmp.cnt,
tmp.content,
row_number() over(distribute by tmp.user_id,tmp.type sort by tmp.cnt desc) AS rn,
current_timestamp AS dw_date
FROM
(SELECT
user_id,
'visit_ip' AS type,
sum(pv) AS cnt,
visit_ip AS content
FROM    qfbap_dwd.dwd_user_pc_pv
GROUP BY
user_id,visit_ip
UNION ALL
SELECT
user_id,
'cookie_id' AS type,
sum(pv) AS cnt,
cookie_id AS content
FROM    qfbap_dwd.dwd_user_pc_pv
GROUP BY
user_id,cookie_id
UNION ALL
SELECT
user_id,
'browser_name' AS type,
sum(pv) AS cnt,
browser_name AS content
FROM    qfbap_dwd.dwd_user_pc_pv
GROUP BY
user_id,browser_name
UNION ALL
SELECT
user_id,
'visit_os' AS type,
sum(pv) AS cnt,
visit_os AS content
FROM    qfbap_dwd.dwd_user_pc_pv
GROUP BY
user_id,visit_os) tmp)tmp2
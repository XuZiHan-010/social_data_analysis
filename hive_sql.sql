----------Data Loading and Preparations------
create database database_message;

use database_message;

--drop table in case it already exists
drop table if exists database_message.table_message_source;

--create the table msg_source
create table database_message.table_message_source(
    msg_time string comment "发送时间 message sent time",
    sender_name string comment "发送人昵称 sender's nickname",
    sender_account string comment "发送人账号 sender's account",
    sender_sex string comment "发送人性别 sender's gender",
    sender_ip string comment "发送人ip地址 sender's ipaddress",
    sender_os string comment "发送人操作系统 sender's operating system",
    sender_phonetype string comment "发送人手机型号 sender's phone type",
    sender_network string comment "发送人网络类型 sender's network type",
    sender_gps string comment "发送人的GPS定位 sender's GPS location",
    receiver_name string comment "接收人昵称 recipient's nickname",
    receiver_ip string comment "接收人IP recipient's ipaddress",
    receiver_account string comment "接收人账号 recipient's account",
    receiver_os string comment "接收人操作系统 recipient's operating system",
    receiver_phonetype string comment "接收人手机型号 recipient's phone type",
    receiver_network string comment "接收人网络类型 recipient's network type",
    receiver_gps string comment "接收人的GPS定位 recipient's GPS location",
    receiver_sex string comment "接收人性别 recipient's gender",
    msg_type string comment "消息类型 message type",
    distance string comment "双方距离 distance between recipient and sender",
    message string comment "消息内容 content of message"
);

--load the data to the msg table
LOAD DATA LOCAL INPATH '/home/hadoop/simulated_social_data.csv' overwrite INTO TABLE table_message_source;



-- select 100 sample rows to verify the data has been loaded successfully
select * from table_message_source tablesample ( 100 rows );

-- verify whether the size of the data is 1000,000
select count(*) from table_message_source;

------------ ETL--------

-- create the new table which stores the filtered data
create table database_message.table_message_etl(
    msg_time string comment "发送时间 message sent time",
    sender_name string comment "发送人昵称 sender's nickname",
    sender_account string comment "发送人账号 sender's account",
    sender_sex string comment "发送人性别 sender's gender",
    sender_ip string comment "发送人ip地址 sender's ipaddress",
    sender_os string comment "发送人操作系统 sender's operating system",
    sender_phonetype string comment "发送人手机型号 sender's phone type",
    sender_network string comment "发送人网络类型 sender's network type",
    sender_gps string comment "发送人的GPS定位 sender's GPS location",
    receiver_name string comment "接收人昵称 recipient's nickname",
    receiver_ip string comment "接收人IP recipient's ipaddress",
    receiver_account string comment "接收人账号 recipient's account",
    receiver_os string comment "接收人操作系统 recipient's operating system",
    receiver_phonetype string comment "接收人手机型号 recipient's phone type",
    receiver_network string comment "接收人网络类型 recipient's network type",
    receiver_gps string comment "接收人的GPS定位 recipient's GPS location",
    receiver_sex string comment "接收人性别 recipient's gender",
    msg_type string comment "消息类型 message type",
    distance string comment "双方距离 distance between recipient and sender",
    message string comment "消息内容 content of message",
    msg_day string comment "发送消息日 message sent day",
    msg_hour string comment "发送消息小时 message sent hour",
    sender_long double comment "发送者经度 sender's longitude",
    sender_lat double comment "发送者纬度 sender's latitude"
);

-- filter the data which is empty or nullselect * from tb_msg_source
-- and insert the filtered data to the new table db_msg.tb_msg_etl
INSERT OVERWRITE TABLE database_message.table_message_etl
SELECT *,date(msg_time) as msg_day,hour(msg_time), -- extract the date and hour from msg_time
       split(sender_gps,',')[0] as sender_long, --extract the longitude from sender's gps
       split(sender_gps,',')[1] as sender_lat --extract the latitude from sender's gps
FROM database_message.table_message_source
WHERE
    --the below selected columns are those shouldn't be null or empty
    msg_time IS NOT NULL AND LENGTH(msg_time) > 0
    AND sender_name IS NOT NULL AND LENGTH(sender_name) > 0
    AND sender_account IS NOT NULL AND LENGTH(sender_account) > 0
    AND sender_sex IS NOT NULL AND LENGTH(sender_sex) > 0
    AND sender_ip IS NOT NULL AND LENGTH(sender_ip) > 0
    AND sender_os IS NOT NULL AND LENGTH(sender_os) > 0
    AND sender_phonetype IS NOT NULL AND LENGTH(sender_phonetype) > 0
    AND sender_network IS NOT NULL AND LENGTH(sender_network) > 0
    AND sender_gps IS NOT NULL AND LENGTH(sender_gps) > 0
    AND receiver_name IS NOT NULL AND LENGTH(receiver_name) > 0
    AND receiver_account IS NOT NULL AND LENGTH(receiver_account) > 0
    AND receiver_sex IS NOT NULL AND LENGTH(receiver_sex) > 0
    AND msg_type IS NOT NULL AND LENGTH(msg_type) > 0
    AND distance IS NOT NULL AND LENGTH(distance) > 0
    AND message IS NOT NULL AND LENGTH(message) > 0
    -- following columns can be null
    AND (receiver_ip IS NOT NULL AND LENGTH(receiver_ip) > 0 OR receiver_ip IS NULL)
    AND (receiver_os IS NOT NULL AND LENGTH(receiver_os) > 0 OR receiver_os IS NULL)
    AND (receiver_phonetype IS NOT NULL AND LENGTH(receiver_phonetype) > 0 OR receiver_phonetype IS NULL)
    AND (receiver_network IS NOT NULL AND LENGTH(receiver_network) > 0 OR receiver_network IS NULL)
    AND (receiver_gps IS NOT NULL AND LENGTH(receiver_gps) > 0 OR receiver_gps IS NULL);



-- total message amount daily count 统计每日消息总量 --
create table database_message.table_message_totoal_message_count as
select msg_day, count(*) as total_message_count from database_message.table_message_etl group by msg_day ;


-- hourly message amount count and the count of senders, recipients--
-- 统计每小时的消息量，发送和接收的用户数量--
create table database_message.table_hourly_message_count as
select table_message_etl.msg_hour,
       count(*) as total_message_count,
       count(distinct sender_account) as sender_user_count,
       count(distinct receiver_account) as receiver_user_count
from database_message.table_message_etl
group by msg_hour;

-- message amount count for each district统计各地区的消息发送量 --

-- group the data by the province coordination --
-- get the range for latitude and longitude for each province by geojson data china_province.json--
CREATE TABLE database_message.table_district_long_lat AS
SELECT
    msg_day,
    sender_long,
    sender_lat,
    COUNT(*) AS total_message_count,
    CASE
        WHEN sender_long BETWEEN 115.13391113 AND 119.6321106
            AND sender_lat BETWEEN 29.39522171 AND 34.64086914 THEN 'Anhui'
        WHEN sender_long BETWEEN 115.44081879 AND 117.37973785
            AND sender_lat BETWEEN 39.44854355 AND 40.97891235 THEN 'Beijing'
        WHEN sender_long BETWEEN 105.28674316 AND 110.19042969
            AND sender_lat BETWEEN 28.16412735 AND 32.21204376 THEN 'Chongqing'
        WHEN sender_long BETWEEN 115.85150909 AND 120.4345932
            AND sender_lat BETWEEN 23.61819267 AND 28.2922821 THEN 'Fujian'
        WHEN sender_long BETWEEN 92.76416016 AND 108.70762634
            AND sender_lat BETWEEN 32.59889603 AND 42.77169037 THEN 'Gansu'
        WHEN sender_long BETWEEN 109.67652893 AND 117.18708038
            AND sender_lat BETWEEN 20.25403023 AND 25.48793221 THEN 'Guangdong'
        WHEN sender_long BETWEEN 104.53002167 AND 112.05690002
            AND sender_lat BETWEEN 21.40263939 AND 26.33722878 THEN 'Guangxi'
        WHEN sender_long BETWEEN 103.6348114 AND 109.55052948
            AND sender_lat BETWEEN 24.62371445 AND 29.15673256 THEN 'Guizhou'
        WHEN sender_long BETWEEN 108.63458252 AND 110.9826355
            AND sender_lat BETWEEN 18.29375076 AND 20.09542084 THEN 'Hainan'
        WHEN sender_long BETWEEN 113.49282074 AND 119.80721283
            AND sender_lat BETWEEN 36.12546539 AND 42.61648941 THEN 'Hebei'
        WHEN sender_long BETWEEN 121.19284058 AND 134.77156067
            AND sender_lat BETWEEN 43.4787178 AND 53.56085968 THEN 'Heilongjiang'
        WHEN sender_long BETWEEN 110.35552216 AND 116.63354492
            AND sender_lat BETWEEN 31.40503883 AND 36.36000061 THEN 'Henan'
        WHEN sender_long BETWEEN 108.40393829 AND 116.12962341
            AND sender_lat BETWEEN 29.04828072 AND 33.25967407 THEN 'Hubei'
        WHEN sender_long BETWEEN 108.90032196 AND 114.24922943
            AND sender_lat BETWEEN 24.73996162 AND 30.12800407 THEN 'Hunan'
        WHEN sender_long BETWEEN 116.36860657 AND 121.85041809
            AND sender_lat BETWEEN 30.76718521 AND 35.08986282 THEN 'Jiangsu'
        WHEN sender_long BETWEEN 113.60186005 AND 118.4661026
            AND sender_lat BETWEEN 24.48704147 AND 29.95217133 THEN 'Jiangxi'
        WHEN sender_long BETWEEN 121.66998291 AND 131.2759552
            AND sender_lat BETWEEN 40.8931427 AND 46.30136871 THEN 'Jilin'
        WHEN sender_long BETWEEN 118.83559418 AND 125.91251373
            AND sender_lat BETWEEN 38.72541809 AND 43.37086868 THEN 'Liaoning'
        WHEN sender_long BETWEEN 97.1847229 AND 126.06407166
            AND sender_lat BETWEEN 37.40818787 AND 53.38851166 THEN 'Nei Mongol'
        WHEN sender_long BETWEEN 104.27907562 AND 107.65174866
            AND sender_lat BETWEEN 35.41488647 AND 39.35830688 THEN 'Ningxia Hui'
        WHEN sender_long BETWEEN 89.49705505 AND 102.99154663
            AND sender_lat BETWEEN 31.6777401 AND 39.30706024 THEN 'Qinghai'
        WHEN sender_long BETWEEN 105.4953537 AND 111.20724487
            AND sender_lat BETWEEN 31.73316765 AND 39.48997116 THEN 'Shaanxi'
        WHEN sender_long BETWEEN 114.84293365 AND 122.57125092
            AND sender_lat BETWEEN 34.39336014 AND 38.25597382 THEN 'Shandong'
        WHEN sender_long BETWEEN 120.89511871 AND 121.89041901
            AND sender_lat BETWEEN 30.68958282 AND 31.50541687 THEN 'Shanghai'
        WHEN sender_long BETWEEN 110.27056122 AND 114.54537201
            AND sender_lat BETWEEN 34.5941391 AND 40.73575974 THEN 'Shanxi'
        WHEN sender_long BETWEEN 97.36296082 AND 108.53423309
            AND sender_lat BETWEEN 26.09018707 AND 34.30934525 THEN 'Sichuan'
        WHEN sender_long BETWEEN 116.75075531 AND 118.01268768
            AND sender_lat BETWEEN 38.56954575 AND 40.22687149 THEN 'Tianjin'
        WHEN sender_long BETWEEN 73.65153503 AND 96.36516571
            AND sender_lat BETWEEN 34.37163544 AND 49.10752106 THEN 'Xinjiang Uygur'
        WHEN sender_long BETWEEN 78.46431732 AND 99.11231232
            AND sender_lat BETWEEN 27.56496048 AND 36.48248672 THEN 'Xizang'
        WHEN sender_long BETWEEN 97.55757904 AND 106.18795013
            AND sender_lat BETWEEN 21.25073051 AND 29.25111961 THEN 'Yunnan'
        WHEN sender_long BETWEEN 118.0345993 AND 121.95041656
            AND sender_lat BETWEEN 27.17173386 AND 31.17524719 THEN 'Zhejiang'
        ELSE
            CASE
                WHEN sender_long BETWEEN -180 AND 180 AND sender_lat BETWEEN -90 AND 90 THEN 'Overseas'
                ELSE 'Invalid'
            END
    END AS location_status
FROM database_message.table_message_etl
GROUP BY msg_day, sender_long, sender_lat;



-- daily count of senders and recipients --
create table database_message.table_user_count as
select
    msg_day,
    count(distinct table_message_etl.sender_account) as sender_count,
    count(distinct table_message_etl.receiver_account) as reciever_count
from database_message.table_message_etl
group by msg_day;




-- count the top5 users who send the most information monthly --

CREATE TABLE database_message.table_top5_senders_per_month AS
WITH monthly_sender_counts AS (
    SELECT
        sender_name,
        YEAR(msg_day) AS year,
        MONTH(msg_day) AS month,
        COUNT(*) AS sender_message_count
    FROM database_message.table_message_etl
    GROUP BY sender_name, YEAR(msg_day), MONTH(msg_day)
),
ranked_senders AS (
    SELECT
        sender_name,
        year,
        month,
        sender_message_count,
        ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY sender_message_count DESC) AS rank
    FROM monthly_sender_counts
)
SELECT
    sender_name,
    year,
    month,
    sender_message_count
FROM ranked_senders
WHERE rank <= 5
ORDER BY year, month, rank;

-- count the top5 users who receive the most information monthly --
CREATE TABLE database_message.table_top5_receivers_per_month AS
WITH monthly_receiver_counts AS (
    SELECT
        receiver_name,
        YEAR(msg_day) AS year,
        MONTH(msg_day) AS month,
        COUNT(*) AS receiver_message_count
    FROM database_message.table_message_etl
    GROUP BY receiver_name, YEAR(msg_day), MONTH(msg_day)
),
ranked_receivers AS (
    SELECT
        receiver_name,
        year,
        month,
        receiver_message_count,
        ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY receiver_message_count DESC) AS rank
    FROM monthly_receiver_counts
)
SELECT
    receiver_name,
    year,
    month,
    receiver_message_count
FROM ranked_receivers
WHERE rank <= 5
ORDER BY year, month, rank;




-- count the distribution of sender's phone type --
create table database_message.table_sender_phonetype as
select
    sender_phonetype,
count(*) as count
from database_message.table_message_etl group by sender_phonetype;





-- count of the phone operating system for each sender --
create table database_message.table_sender_operatingsystem as
select
    table_message_etl.sender_os,
    count(*) as count
from database_message.table_message_etl group by table_message_etl.sender_os;
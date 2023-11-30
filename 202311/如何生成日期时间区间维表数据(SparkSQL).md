#### 参考生成日期范围(基于POSEXPLODE)
SELECT 
	DATE_ADD(START_DATE, pos),
    pos,
    null_ele
FROM (
	SELECT DISTINCT
		"2023-03-13" AS START_DATE,
		"2023-03-23" AS END_DATE	
) lateral VIEW posexplode(split(SPACE(DATEDIFF(END_DATE, START_DATE)), " ")) s2 AS pos, null_ele


#### 生成日期时间维度表 参考SPARKSQL
WITH date_base AS
(
SELECT
	 to_timestamp('2017-02-14 16:08:47','yyyy-MM-dd HH:mm:ss') + make_interval(0,0,0,0,0,0,t2.pots) as date_dim_key
FROM
	(
	SELECT
		DISTINCT 
		unix_timestamp('2017-02-14 16:08:47','yyyy-MM-dd HH:mm:ss') as sts,
		unix_timestamp('2017-02-14 17:35:11','yyyy-MM-dd HH:mm:ss') as ets
    ) s1 LATERAL view posexplode(split(SPACE((ets-sts)),SPACE(1))) t2 AS pots,valts
)
SELECT
	date_dim_key,
	date_dim_key as real_time,
	SECOND(date_dim_key) as seconds_id,
	MINUTE(date_dim_key) as minute_id,
	HOUR(date_dim_key) as hour_id,
	DAY(date_dim_key) as day_id,
	MONTH(date_dim_key) as month_id,
	YEAR(date_dim_key) as year_id
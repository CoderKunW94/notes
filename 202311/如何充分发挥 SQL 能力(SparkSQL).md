# 如何充分发挥 SQL 能力(SparkSQL)

参考来自 https://developer.aliyun.com/article/1360381

## 数列

数列是最常见的数据形式之一，实际数据开发场景中遇到的基本都是有限数列。本节将从最简单的递增数列开始，找出一般方法并推广到更泛化的场景。

**1.1 常见数列**

- 1.1.1 一个简单的递增数列
首先引出一个简单的递增整数数列场景  
 • 从数值 0 开始  
 • 之后的每个数值递增 1  
 • 至数值 3 结束  
如何生成满足以上三个条件的数列？即 [0,1,2,3] 。
实际上，生成该数列的方式有多种，此处介绍其中一种简单且通用的方案

SQL实现

````sql
SELECT    
	t.pos AS a_n 
	FROM (
		SELECT POSEXPLODE(SPLIT(SPACE(3), SPACE(1)))
	) t;

````

通过上述 SQL 片段可得知，生成一个递增序列只需要三个步骤：  
- 1）生成一个长度合适的数组，数组中的元素不需要具有实际含义；  
- 2）通过 UDTF 函数 posexplode 对数组中的每个元素生成索引下标；  
- 3）取出每个元素的索引下标。以上三个步骤可以推广至更一般的数列场景：等差数列、等比数列。下文将以此为基础，直接给出最终实现模板

**等差数列**

SQL实现
 
 ```` sql
 SELECT    
 	a + t.pos * d AS a_n
	FROM (    
		SELECT POSEXPLODE(SPLIT(SPACE(n - 1), SPACE(1)))
		) t;
 ````

**等比数列**

 SQL实现

 ```sql
 SELECT 
 	a * pow(q, t.pos) AS a_n
	FROM (    
		SELECT POSEXPLODE(SPLIT(SPACE(n - 1), SPACE(1)))
		) t;
 
 ```
 提示：亦可直接使用SparkSQL系统函数 sequence 快速生成数列
 
 ```sql
 -- SQL - 4
 SELECT SEQUENCE(1, 3, 1);
-- result[1, 2, 3]
 ```

**应用场景举例**

还原任意维度组合下的维度列簇名称  

在多维分析场景下，可能会用到高阶聚合函数，如 cube 、 rollup 、 grouping sets   等，可以针对不同维度组合下的数据进行聚合统计  

场景描述

现有用户访问日志表 visit_log ，每一行数据表示一条用户访问日志

``` sql
-- visit_log 
WITH visit_log AS (
    SELECT STACK (
        6,
        '2024-01-01', '101', '湖北', '武汉', 'Android',
        '2024-01-01', '102', '湖南', '长沙', 'IOS',
        '2024-01-01', '103', '四川', '成都', 'Windows',
        '2024-01-02', '101', '湖北', '孝感', 'Mac',
        '2024-01-02', '102', '湖南', '邵阳', 'Android',
        '2024-01-03', '101', '湖北', '武汉', 'IOS'
    ) 
    -- 字段：日期，用户，省份，城市，设备类型
    AS (dt, user_id, province, city, device_type)
)
SELECT * FROM visit_log;
```

SQL实现  

``` sql
WITH group_dimension AS (
SELECT        
	gb.group_id,
	 -- 每种分组对应的维度字段 
	CONCAT_WS(',',COLLECT_LIST(
		CASE 
			WHEN gb.placeholder_bit = 0 
			THEN dim_col.val 
		ELSE null END)
		) AS dimension_name
FROM
	(
	SELECT
		groups.pos AS group_id,
		pe.*
	FROM
		(SELECT POSEXPLODE(SPLIT(SPACE(CAST(POW(2,3) AS INT) - 1),SPACE(1)))) groups 
		LATERAL VIEW POSEXPLODE(REGEXP_EXTRACT_ALL(LPAD(CONV(groups.pos, 10, 2), 3, '0'),'(0|1)')) pe AS placeholder_idx,placeholder_bit
	 ) gb
LEFT JOIN (
	SELECT POSEXPLODE(SPLIT('省份,城市,设备类型',',')) AS (pos,val)
	) 
	dim_col 
	ON gb.placeholder_idx = dim_col.pos
GROUP BY
	gb.group_id
)
SELECT
	group_dimension.dimension_name,
	province,
	city,
	device_type,
	visit_count
	FROM 
	(
	SELECT
		GROUPING_ID(province,city,device_type) AS group_id,
		province,
		city,
		device_type,
		COUNT(1) AS visit_count
	FROM
		t_visit_log b
	GROUP BY
		province,city,device_type 
		GROUPING SETS( 
		(province),
		(province,city),
		(province,city,device_type) 
		)
	) t 
	JOIN 
	group_dimension ON t.group_id = group_dimension.group_id 
	ORDER BY group_dimension.dimension_name;
```  

## 区间

**区间分割**

```sql
SELECT
	a + t.pos * d AS sub_interval_start,
	-- 子区间起始值    
	a + (t.pos + 1) * d AS sub_interval_end
	-- 子区间结束值 
FROM
	(
		SELECT POSEXPLODE(SPLIT(SPACE(n - 1),SPACE(1)))
	) t
```

**区间交叉**

```sql
WITH dummy_table AS 
(
SELECT
	STACK(2,
	'2024-01-01',
	'2024-01-03',
	'2024-01-02',
	'2024-01-04' 
	) AS (date_start,date_end)
)
SELECT
	MIN(date_item) AS date_start_merged,
	MAX(date_item) AS date_end_merged,
	collect_set(
	-- 交叉日期计数        
     CASE
		WHEN date_item_cnt > 1 THEN CONCAT(date_item, ':', date_item_cnt)
		ELSE null
	end    
) AS overlap_date 
FROM (
	SELECT
		-- 拆解后的单个日期        
		DATE_ADD(date_start, pos) AS date_item,
		-- 拆解后的单个日期出现的次数        
		COUNT(1) OVER (PARTITION BY DATE_ADD(date_start, pos)
		) AS date_item_cnt
	FROM
    	dummy_table 
		LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(date_end, date_start)),SPACE(1),0)) t AS pos,val
	) t

```

🤔增加点儿难度！  
如果有多个日期区间，且区间之间交叉状态未知，上述问题又该如何求解。即：
1）如何合并多个日期区间，并返回合并后的多个新区间？
2）如何知道哪些日期是交叉日期，并返回该日期交叉次数？

```sql
WITH dummy_table AS (
SELECT
	stack(5,
	'2024-01-01','2024-01-03',
	'2024-01-02','2024-01-04',
	'2024-01-06','2024-01-08',
	'2024-01-08','2024-01-08',
	'2024-01-07','2024-01-10'    
) AS (date_start,date_end)
)
SELECT
	MIN(date_item) AS date_start_merged,
	MAX(date_item) AS date_end_merged,
	COLLECT_SET(
	-- 交叉日期计数        
	CASE
		WHEN date_item_cnt > 1 THEN CONCAT(date_item, ':', date_item_cnt)
		ELSE NULL
	END    
) AS overlap_date
FROM
	(
	SELECT
		-- 拆解后的单个日期        
		DATE_ADD(date_start, pos) AS date_item,
		-- 拆解后的单个日期出现的次数        
		COUNT(1) OVER (PARTITION BY DATE_ADD(date_start, pos)) AS date_item_cnt,
		-- 对于拆解后的单个日期，重组为新区间的标记        
		DATE_ADD(DATE_ADD(date_start, pos), 1 - DENSE_RANK() OVER (ORDER BY DATE_ADD(date_start, pos))) AS cont
	FROM
		dummy_table    
		LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(date_end, date_start)),SPACE(1))) t AS pos,
		val) t
GROUP BY cont;
```

**应用场景举例**

按任意时段统计数据

**场景描述**  

现有用户还款计划表 user_repayment ，该表内的一条数据，表示用户在指定日期区间内 [date_start, date_end] ，每天还款 repayment 元

```sql
WITH user_repayment AS 
(    
    SELECT 
        stack(3,
            '101', '2024-01-01', '2024-01-15', 10,
            '102', '2024-01-05', '2024-01-20', 20, 
            '103', '2024-01-10', '2024-01-25', 30    
            )     
            -- 字段：用户，开始日期，结束日期，每日还款金额    
            AS (user_id, date_start, date_end, repayment)
)
SELECT * FROM user_repayment;
```

如何统计任意时段内（如：2024-01-15至2024-01-16）每天所有用户的应还款总额?

**解决思路**

核心思路是将日期区间转换为日期序列，再按日期序列进行汇总统计。

**SQL 实现**  
````sql
SELECT
	date_item AS day,
	sum(repayment) AS total_repayment
FROM
	(
	SELECT
		date_add(date_start, pos) AS date_item,
		repayment
	FROM t_user_repayment
    LATERAL view posexplode(split(SPACE(datediff(date_end, date_start)),SPACE(1))) t AS pos,val
    ) t
WHERE
	date_item >= '2024-01-15'
	AND 
    date_item <= '2024-01-16'
GROUP BY
	date_item
ORDER BY
	date_item
````

## 排列组合  
排列组合是针对离散数据常用的数据组织方法，本节将分别介绍排列、组合的实现方法，并结合实例着重介绍通过组合对数据的处理。

常见排列组合操作

**排列**   
已知字符序列 [ 'A', 'B', 'C' ] ，每次从该序列中可重复地选取出 2 个字符，如何获取到所有的排列  
借助多重 `lateral view` 即可解决，整体实现比较简单。


``` sql
SELECT
	concat(val1, val2) AS perm
FROM
	(
	SELECT
		split('A,B,C',',') AS CHARACTERS
    ) dummy
    LATERAL view explode(CHARACTERS) t1 AS val1
    LATERAL view explode(CHARACTERS) t2 AS val2;
```


**组合**
已知字符序列 [ 'A', 'B', 'C' ] ，每次从该序列中可重复地选取出 2 个字符，如何获取到所有的组合  
借助多重 `lateral view` 即可解决，整体实现比较简单

```sql
SELECT
	CONCAT(LEAST(val1, val2), GREATEST(val1, val2)) AS comb 
FROM (
	SELECT
		SPLIT('A,B,C',',') AS CHARACTERS) dummy
		LATERAL VIEW EXPLODE(CHARACTERS) t1 AS val1
		LATERAL VIEW EXPLODE(CHARACTERS) t2 AS val2
		GROUP BY least(val1,val2),
	GREATEST(val1,val2)
```

**应用场景举例**

分组对比统计  

**场景描述**  

现有投放策略转化表，该表内的一条数据，表示一天内某投放策略带来的订单量。

``` sql
WITH strategy_order AS 
(
	SELECT
		stack( 3,
		'2024-01-01','Strategy A',10,
		'2024-01-01','Strategy B',20,
		'2024-01-01','Strategy C',30 
		)
	-- 字段：日期，投放策略，单量    
AS (dt,
	strategy,
	order_cnt
	)
)
SELECT dt,strategy,order_cnt FROM strategy_order
```

如何按投放策略建立两两对比组，按组对比展示不同策略转化单量情况  

解决思路 

核心思路是从所有投放策略列表中不重复地取出 2 个策略，生成所有的组合结果，然后关联 `strategy_order` 表分组统计结果

SQL 实现

```sql
SELECT
	/*+ mapjoin(combs) */
	combs.strategy_comb,
	so.strategy,
	so.order_cnts
FROM
	t_strategy_order so
JOIN (
	-- 生成所有对比组    
	SELECT
		concat(least(val1, val2), '-', greatest(val1, val2)) AS strategy_comb,
		least(val1,val2) AS strategy_1,
		greatest(val1,val2) AS strategy_2
	FROM
		(
		SELECT
			collect_set(strategy) AS strategies
		FROM
			t_strategy_order ) dummy    
LATERAL view explode(strategies) t1 AS val1    
LATERAL view explode(strategies) t2 AS val2
	WHERE
		val1 <> val2
	GROUP BY
		least(val1,val2),
		greatest(val1,val2)
	) combs 
ON
	1 = 1
WHERE
	so.strategy IN (combs.strategy_1, combs.strategy_2)
ORDER BY
	combs.strategy_comb,
	so.strategy
```

## 连续  
本节主要介绍连续性问题，重点描述了常见连续活跃场景。对于静态类型的连续活跃、动态类型的连续活跃，分别阐述了不同的实现方案

5.1 普通连续活跃统计
场景描述

现有用户访问日志表 `visit_log` ，每一行数据表示一条用户访问日志


如何获取连续访问大于或等于 2 天的用户

上述问题在分析连续性时，获取连续性的结果以超过固定阈值为准，此处归类为 连续活跃大于 N 天阈值的普通连续活跃场景统计。

SQL 实现  

**基于相邻日期差实现（ lag / lead 版）**

整体实现比较简单

```sql
SELECT
	user_id
FROM
	(
	SELECT
		*,
		LAG(dt,2 - 1) OVER (PARTITION BY user_id ORDER BY dt) AS lag_dt
	FROM
		(
		SELECT
			dt,
			user_id
		FROM
			t_visit_log
		GROUP BY dt,user_id
		) t0    
    ) t1
WHERE
	DATEDIFF(dt, lag_dt) + 1 = 2
GROUP BY user_id;
```
**基于相邻日期差实现（排序版）**

整体实现比较简单

````sql
SELECT
	user_id
FROM
	(
	SELECT
		*,
		DENSE_RANK() OVER (PARTITION BY user_id ORDER BY dt) AS dr
	FROM
		t_visit_log
    ) t1
WHERE
	DATEDIFF(dt, DATE_ADD(dt, 1 - dr)) + 1 = 2
GROUP BY user_id
````

**基于连续活跃天数实现**  

可以视作 基于相邻日期差实现（排序版） 的衍生版本 
该实现能获取到更多信息，如连续活跃天数

```sql
SELECT
	user_id
FROM
	(
	SELECT
		*,
		-- 连续活跃天数        
		COUNT(dt) OVER (PARTITION BY user_id,cont) AS cont_days
	FROM
		(
		SELECT
			*,
			DATE_ADD(dt, 1 - DENSE_RANK() OVER (PARTITION BY user_id ORDER BY dt)) AS cont
		FROM
			t_visit_log    
         ) 
        t1  
        ) t2
WHERE cont_days >= 2
GROUP BY user_id
```

**基于连续活跃区间实现**  

可以视作 基于相邻日期差实现（排序版） 的衍生版本，该实现能获取到更多信息，如连续活跃区间。

``` sql
SELECT
	user_id
FROM
	(
	SELECT
		user_id,
		cont,
		-- 连续活跃区间        
		MIN(dt) AS cont_date_start,
		MAX(dt) AS cont_date_end
	FROM
		(
		SELECT
			*,
			date_add(dt, 1 - DENSE_RANK() OVER (PARTITION BY user_id ORDER BY dt)) AS cont
		FROM
			t_visit_log ) t1
	GROUP BY
		user_id,
		cont) t2
WHERE
	DATEDIFF(cont_date_end, cont_date_start) + 1 >= 2
GROUP BY
	user_id
```

动态连续活跃统计

如何获取最长的 2 个连续活跃用户，输出用户、最长连续活跃天数、最长连续活跃日期区间？
上述问题在分析连续性时，获取连续性的结果不是且无法与固定的阈值作比较，而是各自以最长连续活跃作为动态阈值，此处归类为 动态连续活跃场景统计。

SQL 实现
基于 普通连续活跃场景统计 的思路进行扩展即可，此处直接给出最终 SQL ：

```sql
SELECT
	user_id,
	-- 最长连续活跃天数    
	datediff(max(dt), min(dt)) + 1 AS cont_days,
	-- 最长连续活跃日期区间 
	min(dt) AS cont_date_start,
	max(dt) AS cont_date_end
FROM
	(
	SELECT
		*,
		date_add(dt, 1 - DENSE_RANK() OVER (PARTITION BY user_id ORDER BY dt)) AS cont
	FROM
		t_visit_log) t1
GROUP BY
	user_id,
	cont
ORDER BY
	cont_days DESC 
    limit 2
```

## 扩展

引申出更复杂的场景，是本篇文章前面章节内容的结合与变种  

- 区间连续（最长子区间切分）


场景描述

现有用户扫描或连接 WiFi 记录表 user_wifi_log ，每一行数据表示某时刻用户扫描或连接 WiFi 的日志

```sql
with user_wifi_log as 
(    
    select 
        stack (9, 
        '2024-01-01 10:01:00', '101', 'cmcc-Starbucks', 'scan',
        '2024-01-01 10:02:00', '101', 'cmcc-Starbucks', 'scan',
        '2024-01-01 10:03:00', '101', 'cmcc-Starbucks', 'scan',
        '2024-01-01 10:04:00', '101', 'cmcc-Starbucks', 'conn', 
        '2024-01-01 10:05:00', '101', 'cmcc-Starbucks', 'conn',
        '2024-01-01 10:06:00', '101', 'cmcc-Starbucks', 'conn',
        '2024-01-01 11:01:00', '101', 'cmcc-Starbucks', 'conn', 
        '2024-01-01 11:02:00', '101', 'cmcc-Starbucks', 'conn', 
        '2024-01-01 11:03:00', '101', 'cmcc-Starbucks', 'conn'    
        )     -- 字段：时间，用户，WiFi，状态（扫描、连接）    
        AS (time, user_id, wifi, status)
)
select * from user_wifi_log

```


现需要进行用户行为分析，如何划分用户不同 WiFi 行为区间？满足：1）行为类型分为两种：连接（scan）、扫描（conn）；2）行为区间的定义为：相同行为类型，且相邻两次行为的时间差不超过 30 分钟；
3）不同行为区间在满足定义的情况下应取到最长；

上述问题稍显复杂，可视作 动态连续活跃统计 中介绍的 最长连续活跃 的变种。可以描述为 结合连续性阈值与行为序列中的上下文信息，进行最长子区间的划分 的问题

SQL 实现  

核心逻辑：以用户、WIFI 分组，结合连续性阈值与行为序列上下文信息，划分行为区间

详细步骤  
- 1）以用户、WIFI 分组，在分组窗口内对数据按时间正序排序
- 2）依次遍历分组窗口内相邻两条记录，若两条记录之间的时间差超过 30 分钟，或者两条记录的行为状态（扫描态、连接态）发生变更，则以该临界点划分行为区间。直到遍历所有记录；
- 3）最终输出结果：用户、WIFI、行为状态（扫描态、连接态）、行为开始时间、行为结束时间；

```sql
SELECT
	user_id,
	wifi,
	MAX(status) AS status,
	MIN(`time`) AS start_time,
	MAX(`time`) AS end_time 
	from (
	SELECT
		*,
		MAX(
		IF(lag_status IS NULL OR lag_time IS NULL 
		OR status <> lag_status 
		OR (unix_timestamp(`time`)-unix_timestamp(lag_time))> 60 * 30,rn,null)
		) OVER (PARTITION BY user_id,wifi ORDER BY `time`) 
		AS group_idx
	FROM
		(
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY user_id,wifi ORDER BY `time`) AS rn,
			lag(time,1) OVER (PARTITION BY user_id,wifi ORDER BY `time`) AS lag_time,
			lag(status,1) OVER (PARTITION BY user_id,wifi ORDER BY `time`) AS lag_status
		FROM
			t_user_wifi_log ) t1
	) t2 
	GROUP BY user_id,
	wifi,
	group_idx
````

结语  
该案例中的连续性判别条件可以推广到更多场景，例如基于日期差值、时间差值、枚举类型、距离差值等作为连续性判别条件的数据场景


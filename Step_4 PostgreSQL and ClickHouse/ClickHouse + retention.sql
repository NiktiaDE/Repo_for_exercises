-- Сырые логи событий
CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
)
ENGINE = MergeTree()
ORDER BY
(event_time,
user_id)
TTL event_time + INTERVAL 30 DAY;

-- Агрегированная таблица
CREATE TABLE user_metrics (
	event_time DateTime,
	event_type String,
	unique_users AggregateFunction(uniq, UInt32),
	total_points_spent AggregateFunction(sum, UInt32),
	total_actions AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_time, event_type)
TTL event_time + INTERVAL 180 DAY;

-- Материализованная вьюха 
CREATE MATERIALIZED VIEW user_metrics_mv TO user_metrics AS
SELECT
	event_time,
	event_type,
	uniqState(user_id) AS unique_users,
	sumState(points_spent) AS total_points_spent,
	countState() AS total_actions
FROM user_events
GROUP BY (event_time, event_type);

-- Запрос с группировками по быстрой аналитике по дням
SELECT
	toDate(event_time) AS event_date,
	event_type,
	uniqMerge(unique_users) AS unique_users,
	sumMerge(total_points_spent) AS total_spent,
	countMerge(total_actions) AS final_actions
FROM
	user_metrics
GROUP BY
	event_time,
	event_type
ORDER BY
	event_time,
	event_type;


-- Вычисление ретеншена (вернувшиеся)
SELECT
	user_day_0.day_0,
	COUNT(*) AS total_users_day_0,
	COUNT(DISTINCT ue.user_id) AS returned_in_7_days,
	--Процент удержания за 7 дней, 2 знака после запятой
	round(100.0 * COUNT(DISTINCT ue.user_id) / COUNT(*), 2) AS retention_7d_percent
FROM
	(
	SELECT
		user_id,
		MIN(toDate(event_time)) AS day_0
	FROM
		user_events
	GROUP BY
		user_id
    ) AS user_day_0
LEFT JOIN user_events AS ue
    ON
	user_day_0.user_id = ue.user_id
	AND toDate(ue.event_time) BETWEEN user_day_0.day_0 + 1 AND user_day_0.day_0 + 7
GROUP BY
	user_day_0.day_0
ORDER BY
	user_day_0.day_0;


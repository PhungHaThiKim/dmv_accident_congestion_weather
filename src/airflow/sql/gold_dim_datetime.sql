INSERT INTO gold.dim_datetime (
    time_sk,
    date,
    full_timestamp,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    hour,
    is_weekend,
    is_rush_hour_morning,
    is_rush_hour_evening,
    season
)
WITH
    toDate('2016-01-01') AS start_date,
    toDate('2021-12-31') AS end_date
SELECT
    -- time_sk: số giờ kể từ epoch (UTC)
    toUInt64(intDiv(toUnixTimestamp(ts), 3600)) AS time_sk,

    toDate(ts)               AS date,
    ts                       AS full_timestamp,
    toYear(ts)               AS year,
    toQuarter(ts)            AS quarter,
    toMonth(ts)              AS month,
    formatDateTime(ts, '%b') AS month_name,        -- 'Jan','Feb',...
    toDayOfMonth(ts)         AS day,
    toDayOfWeek(ts)          AS day_of_week,       -- 1=Mon..7=Sun
    formatDateTime(ts, '%a') AS day_name,          -- 'Mon','Tue',...
    toHour(ts)               AS hour,

    toDayOfWeek(ts) IN (6, 7)                       AS is_weekend,           -- Sat=6, Sun=7
    (toHour(ts) BETWEEN 7 AND 9)                    AS is_rush_hour_morning,
    (toHour(ts) BETWEEN 16 AND 19)                  AS is_rush_hour_evening,

    multiIf(
        toMonth(ts) IN (3, 4, 5),   'Spring',       -- Mar,Apr,May
        toMonth(ts) IN (6, 7, 8),   'Summer',       -- Jun,Jul,Aug
        toMonth(ts) IN (9, 10, 11), 'Fall',         -- Sep,Oct,Nov
        'Winter'                                   -- Dec,Jan,Feb
    ) AS season
FROM
(
    -- sinh tất cả (date, hour) trong khoảng
    SELECT
        d,
        h,
        -- d: Date -> DateTime64(0,'UTC') + h giờ
        toDateTime64(d, 0, 'UTC') + toIntervalHour(h) AS ts
    FROM
    (
        -- range ngày
        SELECT
            start_date + day_offset AS d
        FROM
        (
            SELECT number AS day_offset
            FROM numbers(dateDiff('day', start_date, end_date) + 1)
        )
    ) AS days
    CROSS JOIN
    (
        -- 24 giờ trong ngày
        SELECT number AS h
        FROM numbers(24)
    ) AS hours
);
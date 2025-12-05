CREATE TABLE IF NOT EXISTS dim_date_pjm
(
    date        Date,
    date_sk     UInt32,              -- yyyymmdd
    dayofweek   UInt8,               -- 0 = Mon, 6 = Sun (Pandas style)
    weekday     String,              -- 'Monday', 'Tuesday', ...
    quarter     UInt8,               -- 1..4
    month       UInt8,               -- 1..12
    date_offset Int32,               -- 0 = 2002-01-01
    is_weekend  UInt8,               -- 1 = Sat/Sun
    season      String               -- 'Spring','Summer','Fall','Winter'
)
ENGINE = MergeTree
ORDER BY date;




INSERT INTO dim_date_pjm
WITH
    toDate('2002-01-01') AS start_date,
    toDate('2020-12-31') AS end_date
SELECT
    d AS date,
    toUInt32(toYYYYMMDD(d))                                  AS date_sk,

    -- dayofweek: 0 = Monday .. 6 = Sunday (Pandas/Kaggle style)
    ((toDayOfWeek(d) + 6) % 7)                               AS dayofweek,

    -- weekday name
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][
        ((toDayOfWeek(d) + 6) % 7) + 1
    ]                                                        AS weekday,

    toQuarter(d)                                             AS quarter,
    toMonth(d)                                               AS month,

    -- số ngày từ start_date
    dateDiff('day', start_date, d)                           AS date_offset,

    -- is_weekend: 1 nếu Sat/Sun
    if(((toDayOfWeek(d) + 6) % 7) >= 5, 1, 0)                AS is_weekend,

    -- season
    multiIf(
        toMonth(d) IN (3, 4, 5),  'Spring',     -- Mar,Apr,May
        toMonth(d) IN (6, 7, 8),  'Summer',     -- Jun,Jul,Aug
        toMonth(d) IN (9, 10, 11),'Fall',       -- Sep,Oct,Nov
        'Winter'                                -- Dec,Jan,Feb
    )                                                        AS season
FROM
(
    SELECT
        start_date + number AS d
    FROM numbers(dateDiff('day', start_date, end_date) + 1)
);

CREATE TABLE dim_holiday_full
(
    date            Date,
    date_sk         UInt32,        -- yyyymmdd
    holiday_name    String,        -- 'Thanksgiving Day', 'Memorial Day', ...
    holiday_type    String,        -- 'federal', 'state', 'market'
    state_code      String,        -- PA, OH, WV... (NULL cho federal/market)
    description     String         -- optional
)
ENGINE = MergeTree
ORDER BY date;
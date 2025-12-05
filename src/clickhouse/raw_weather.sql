-- 1) CREATE TABLE với partition + order by (data type bình thường)
DROP TABLE IF EXISTS weather_events;
CREATE TABLE IF NOT EXISTS weather_events
(
    `EventId` String,
    `Type` String,
    `Severity` String,

    `StartTime(UTC)` DateTime64(0, 'UTC'),
    `EndTime(UTC)`   DateTime64(0, 'UTC'),

    `Precipitation(in)` Nullable(Float32),

    `TimeZone` String,
    `AirportCode` String,

    `LocationLat` Float64,
    `LocationLng` Float64,

    `City` String,
    `County` String,
    `State` String,
    `ZipCode` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(`StartTime(UTC)`)
ORDER BY (`State`, `City`, `StartTime(UTC)`, `EventId`);


-- 2) INSERT từ CSV
INSERT INTO weather_events
SELECT
    `EventId`,
    `Type`,
    `Severity`,

    
    toDateTime64(`StartTime(UTC)`,   0, 'UTC') AS `StartTime`,
    toDateTime64(`EndTime(UTC)`,   0, 'UTC') AS `EndTime`,

    `Precipitation(in)`,

    `TimeZone`,
    `AirportCode`,

    `LocationLat`,
    `LocationLng`,

    `City`,
    `County`,
    `State`,
    `ZipCode`
FROM file(
  'data/cities_filtered_weather/{new_york,chicago,miami,los_angeles}_weather_2016_2021.csv',
  CSVWithNames
)
SETTINGS
    input_format_csv_detect_header = 1,
    date_time_input_format = 'best_effort',
    schema_inference_make_columns_nullable = 1,
    bool_true_representation = 'True',
    bool_false_representation = 'False';

-- 1) CREATE TABLE với partition + order by (data type bình thường)
DROP TABLE IF EXISTS congestion_events;
CREATE TABLE IF NOT EXISTS congestion_events
(
    `ID` String,
    `Severity` UInt8,

    `Start_Lat` Float64,
    `Start_Lng` Float64,

    `StartTime` DateTime64(0),
    `EndTime`   DateTime64(0),

    `Distance(mi)` Nullable(Float32),

    `DelayFromTypicalTraffic(mins)` Nullable(Float32),
    `DelayFromFreeFlowSpeed(mins)`  Nullable(Float32),

    `Congestion_Speed` String,

    `Description` String,
    `Street` String,
    `City` String,
    `County` String,
    `State` String,
    `Country` String,
    `ZipCode` String,
    `LocalTimeZone` String,

    `WeatherStation_AirportCode` String,
    `WeatherTimeStamp` Nullable(DateTime64(3)),

    `Temperature(F)` Nullable(Float32),
    `WindChill(F)`   Nullable(Float32),
    `Humidity(%)`    Nullable(Float32),
    `Pressure(in)`   Nullable(Float32),
    `Visibility(mi)` Nullable(Float32),
    `WindDir`        String,
    `WindSpeed(mph)` Nullable(Float32),
    `Precipitation(in)` Nullable(Float32),

    `Weather_Event` Nullable(String),
    `Weather_Conditions` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(`StartTime`)
ORDER BY (`State`, `City`, `StartTime`, `ID`);


-- 2) INSERT từ CSV
INSERT INTO congestion_events
SELECT
    `ID`,
    `Severity`,

    `Start_Lat`,
    `Start_Lng`,

    toDateTime64(`StartTime`, 0) AS `Start_Time`,
    toDateTime64(`StartTime`,   0) AS `End_Time`,

    `Distance(mi)`,

    `DelayFromTypicalTraffic(mins)`,
    `DelayFromFreeFlowSpeed(mins)`,

    `Congestion_Speed`,

    `Description`,
    `Street`,
    `City`,
    `County`,
    `State`,
    `Country`,
    `ZipCode`,
    `LocalTimeZone`,

    `WeatherStation_AirportCode`,

    toDateTime64(`WeatherTimeStamp`,   0) AS `WeatherTimeStamp`,

    `Temperature(F)`,
    `WindChill(F)`,
    `Humidity(%)`,
    `Pressure(in)`,
    `Visibility(mi)`,
    `WindDir`,
    `WindSpeed(mph)`,
    `Precipitation(in)`,

    NULLIF(`Weather_Event`, '') AS `Weather_Event`,
    `Weather_Conditions`
FROM file(
  'data/cities_filtered_congestion/{new_york,chicago,miami,los_angeles}_congestion_2016_2021.csv',
  CSVWithNames
)
SETTINGS
    input_format_csv_detect_header = 1,
    date_time_input_format = 'best_effort',
    schema_inference_make_columns_nullable = 1,
    bool_true_representation = 'True',
    bool_false_representation = 'False';

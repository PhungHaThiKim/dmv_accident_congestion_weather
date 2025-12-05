-- 1) CREATE TABLE với partition + order by (data type bình thường)
DROP TABLE IF EXISTS accidents;
DROP TABLE IF EXISTS accidents;

CREATE TABLE IF NOT EXISTS accidents
(
    `ID` String,
    `Source` String,
    `Severity` UInt8,

    -- Local (naive) datetime
    `Start_Time` DateTime64(0),
    `End_Time`   DateTime64(0),

    `Start_Lat` Float64,
    `Start_Lng` Float64,
    `End_Lat`   Nullable(Float64),
    `End_Lng`   Nullable(Float64),

    `Distance(mi)` Nullable(Float32),

    `Description` String,
    `Street` String,
    `City` String,
    `County` String,
    `State` String,
    `Zipcode` String,
    `Country` String,
    `Timezone` String,       -- vẫn giữ để sau này convert khi cần
    `Airport_Code` String,

    -- Local (naive) datetime cho thời tiết đi kèm record
    `Weather_Timestamp` Nullable(DateTime64(0)),
    `Temperature(F)`    Nullable(Float32),
    `Wind_Chill(F)`     Nullable(Float32),
    `Humidity(%)`       Nullable(Float32),
    `Pressure(in)`      Nullable(Float32),
    `Visibility(mi)`    Nullable(Float32),
    `Wind_Direction`    String,
    `Wind_Speed(mph)`   Nullable(Float32),
    `Precipitation(in)` Nullable(Float32),
    `Weather_Condition` String,

    `Amenity` Bool,
    `Bump` Bool,
    `Crossing` Bool,
    `Give_Way` Bool,
    `Junction` Bool,
    `No_Exit` Bool,
    `Railway` Bool,
    `Roundabout` Bool,
    `Station` Bool,
    `Stop` Bool,
    `Traffic_Calming` Bool,
    `Traffic_Signal` Bool,
    `Turning_Loop` Bool,

    `Sunrise_Sunset` String,
    `Civil_Twilight` String,
    `Nautical_Twilight` String,
    `Astronomical_Twilight` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Start_Time)
ORDER BY (State, City, Start_Time, ID);


-- 2) INSERT từ CSV
INSERT INTO accidents
SELECT
    `ID`,
    `Source`,
    `Severity`,
    toDateTime64(`Start_Time`, 0) AS `Start_Time`,
    toDateTime64(`End_Time`, 0) AS `End_Time`,
    `Start_Lat`,
    `Start_Lng`,
    `End_Lat`,
    `End_Lng`,
    `Distance(mi)`,
    `Description`,
    `Street`,
    `City`,
    `County`,
    `State`,
    `Zipcode`,
    `Country`,
    `Timezone`,
    `Airport_Code`,
    toDateTime64(`Weather_Timestamp`, 0) AS `Weather_Timestamp`,
    `Temperature(F)`,
    `Wind_Chill(F)`,
    `Humidity(%)`,
    `Pressure(in)`,
    `Visibility(mi)`,
    `Wind_Direction`,
    `Wind_Speed(mph)`,
    `Precipitation(in)`,
    `Weather_Condition`,
    `Amenity`,
    `Bump`,
    `Crossing`,
    `Give_Way`,
    `Junction`,
    `No_Exit`,
    `Railway`,
    `Roundabout`,
    `Station`,
    `Stop`,
    `Traffic_Calming`,
    `Traffic_Signal`,
    `Turning_Loop`,
    `Sunrise_Sunset`,
    `Civil_Twilight`,
    `Nautical_Twilight`,
    `Astronomical_Twilight`
FROM file(
  'data/cities_filtered_accidents/{new_york,chicago,miami,los_angeles}_accidents_2016_2021.csv',
  CSVWithNames
)
SETTINGS
    input_format_csv_detect_header = 1,
    date_time_input_format = 'best_effort',
    schema_inference_make_columns_nullable = 1,
    bool_true_representation = 'True',
    bool_false_representation = 'False';

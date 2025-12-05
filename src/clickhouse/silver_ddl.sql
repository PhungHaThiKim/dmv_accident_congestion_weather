DROP TABLE IF EXISTS silver.accidents;

CREATE TABLE silver.accidents
(
    ID String,
    Source String,
    Severity UInt8,

    Start_Time_Local DateTime64(0),
    End_Time_Local   DateTime64(0),

    Start_Time_UTC DateTime64(0, 'UTC'),
    End_Time_UTC   DateTime64(0, 'UTC'),

    Start_Lat Float64,
    Start_Lng Float64,
    End_Lat Nullable(Float64),
    End_Lng Nullable(Float64),

    Distance_mi Nullable(Float32),

    Description String,
    Street String,
    City String,
    County String,
    State String,
    Zipcode String,
    Country String,
    Timezone String,
    Airport_Code String,

    Weather_Timestamp_Local Nullable(DateTime64(0)),
    Weather_Timestamp_UTC   Nullable(DateTime64(0, 'UTC')),

    Temperature_F Nullable(Float32),
    Wind_Chill_F Nullable(Float32),
    Humidity_pct Nullable(Float32),
    Pressure_in Nullable(Float32),
    Visibility_mi Nullable(Float32),
    Wind_Direction String,
    Wind_Speed_mph Nullable(Float32),
    Precipitation_in Nullable(Float32),
    Weather_Condition String,

    Amenity Bool,
    Bump Bool,
    Crossing Bool,
    Give_Way Bool,
    Junction Bool,
    No_Exit Bool,
    Railway Bool,
    Roundabout Bool,
    Station Bool,
    Stop Bool,
    Traffic_Calming Bool,
    Traffic_Signal Bool,
    Turning_Loop Bool,

    Sunrise_Sunset String,
    Civil_Twilight String,
    Nautical_Twilight String,
    Astronomical_Twilight String,

    ingest_ts DateTime64(0, 'UTC') DEFAULT now64(0)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(Start_Time_UTC)
ORDER BY (ID);

DROP TABLE IF EXISTS silver.traffic_congestion;

CREATE TABLE silver.traffic_congestion
(
    ID String,
    Severity UInt8,

    Start_Lat Float64,
    Start_Lng Float64,

    Start_Time_Local DateTime64(0),
    End_Time_Local   Nullable(DateTime64(0)),

    Start_Time_UTC DateTime64(0, 'UTC'),
    End_Time_UTC   Nullable(DateTime64(0, 'UTC')),

    Distance_mi Nullable(Float32),

    Delay_From_Typical_Traffic_mins Nullable(Float32),
    Delay_From_Free_Flow_Speed_mins Nullable(Float32),

    Congestion_Speed String,

    Description String,
    Street String,
    City String,
    County String,
    State String,
    Zipcode String,
    Country String,
    Timezone String,

    Airport_Code String,  -- tương đương WeatherStation_AirportCode

    Weather_Timestamp_Local Nullable(DateTime64(3)),
    Weather_Timestamp_UTC   Nullable(DateTime64(3, 'UTC')),

    Temperature_F Nullable(Float32),
    Wind_Chill_F  Nullable(Float32),
    Humidity_pct  Nullable(Float32),
    Pressure_in   Nullable(Float32),
    Visibility_mi Nullable(Float32),
    Wind_Direction String,
    Wind_Speed_mph Nullable(Float32),
    Precipitation_in Nullable(Float32),

    Weather_Event Nullable(String),
    Weather_Condition String,

    ingest_ts DateTime64(0, 'UTC') DEFAULT now64(0)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(Start_Time_UTC)
ORDER BY (State, City, Start_Time_UTC, ID);


DROP TABLE IF EXISTS silver.weather_events;

CREATE TABLE silver.weather_events
(
    Event_ID String,
    AirportCode String,

    City String,
    County String,
    State String,
    Zipcode String,

    StartTime_Local DateTime64(0),
    EndTime_Local   Nullable(DateTime64(0)),

    StartTime_UTC DateTime64(0, 'UTC'),
    EndTime_UTC   Nullable(DateTime64(0, 'UTC')),

    Type String,
    Severity Nullable(UInt8),
    Precipitation Nullable(Float32),
    Visibility Nullable(Float32),

    Start_Lat Nullable(Float64),
    Start_Lng Nullable(Float64),

    Timezone String,

    ingest_ts DateTime64(0, 'UTC') DEFAULT now64(0)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(StartTime_UTC)
ORDER BY (State, City, AirportCode, StartTime_UTC, Type);

DROP TABLE IF EXISTS silver.weather_events;

CREATE TABLE silver.weather_events
(
    Event_ID    String,    
    AirportCode String,

    City    String,
    County  String,
    State   String,
    Zipcode String,

    -- local time: thêm mới, tính từ StartTime(UTC)/EndTime(UTC) + TimeZone
    StartTime_Local DateTime64(0),
    EndTime_Local   Nullable(DateTime64(0)),

    -- UTC time: giữ đúng kiểu như bronze
    StartTime_UTC DateTime64(0, 'UTC'),         
    EndTime_UTC   Nullable(DateTime64(0, 'UTC')),
    Timezone String, 

    -- giữ nguyên kiểu với bronze
    Type       String,       
    Severity   String,       
    Precipitation Float32,

    Start_Lat Float64,      
    Start_Lng Float64, 

    ingest_ts DateTime64(0, 'UTC') DEFAULT now64(0)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(StartTime_UTC)
ORDER BY (State, City, AirportCode, StartTime_UTC, Type);


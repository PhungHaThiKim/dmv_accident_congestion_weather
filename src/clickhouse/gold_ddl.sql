-- =========================
-- GOLD: DIMENSION TABLES
-- =========================

DROP TABLE IF EXISTS gold.dim_datetime;

CREATE TABLE gold.dim_datetime
(
    time_sk              UInt64,                      -- hours since epoch (UTC)
    date                 Date,
    full_timestamp       DateTime64(0, 'UTC'),
    year                 UInt16,
    quarter              UInt8,
    month                UInt8,
    month_name           String,
    day                  UInt8,
    day_of_week          UInt8,                       -- 1=Mon..7=Sun
    day_name             String,
    hour                 UInt8,
    is_weekend           Bool,
    is_rush_hour_morning Bool,
    is_rush_hour_evening Bool,
    season               String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)          -- partition theo tháng
ORDER BY (time_sk);                  -- index chính theo time_sk


-- 2. Dim Location
DROP TABLE IF EXISTS gold.dim_location;

CREATE TABLE gold.dim_location
(
    location_key UInt64,          -- cityHash64(lat,lng,city,state,zipcode)
    start_lat    Float64,
    start_lng    Float64,
    city         String,
    county       String,
    state        String,
    zipcode      String,
    country      String,
    region       String,
    is_urban     Bool
)
ENGINE = ReplacingMergeTree
ORDER BY (location_key);


-- 3. Dim Airport
DROP TABLE IF EXISTS gold.dim_airport;

CREATE TABLE gold.dim_airport
(
    airport_key  UInt64,          -- cityHash64(airport_code)
    airport_code String,
    name         String,
    city         String,
    state        String,
    country      String,
    latitude     Float64,
    longitude    Float64,
    timezone     String
)
ENGINE = ReplacingMergeTree
ORDER BY (airport_key);


-- 4. Dim Weather Type
DROP TABLE IF EXISTS gold.dim_weather_type;

CREATE TABLE gold.dim_weather_type
(
    weather_type_key UInt64,      -- cityHash64(weather_type)
    weather_type     String,
    weather_group    String,
    is_severe        Bool,
    description      String
)
ENGINE = ReplacingMergeTree
ORDER BY (weather_type_key);


-- 5. Dim City Climate
DROP TABLE IF EXISTS gold.dim_city_climate;

CREATE TABLE gold.dim_city_climate
(
    city_climate_key    UInt64,   -- cityHash64(city,state)
    city                String,
    state               String,
    climate_zone        String,
    has_snow_winter     Bool,
    has_wet_dry_pattern Bool,
    notes               String
)
ENGINE = ReplacingMergeTree
ORDER BY (city_climate_key);


-- 6. Dim Infrastructure (từ flags của silver.accidents)
DROP TABLE IF EXISTS gold.dim_infrastructure;

CREATE TABLE gold.dim_infrastructure
(
    infra_key           UInt64,   -- cityHash64(flags)
    has_amenity         Bool,
    has_bump            Bool,
    has_crossing        Bool,
    has_give_way        Bool,
    has_junction        Bool,
    has_no_exit         Bool,
    has_railway         Bool,
    has_roundabout      Bool,
    has_station         Bool,
    has_stop            Bool,
    has_traffic_calming Bool,
    has_traffic_signal  Bool,
    has_turning_loop    Bool
)
ENGINE = ReplacingMergeTree
ORDER BY (infra_key);



-- =========================
-- GOLD: FACT TABLES
-- =========================

-- 7. Fact Accident
DROP TABLE IF EXISTS gold.fact_accident;

CREATE TABLE gold.fact_accident
(
    accident_id       String,
    time_sk           UInt64,
    end_time_sk       UInt64,
    location_key      UInt64,
    airport_key       UInt64,
    infra_key         UInt64,
    city_climate_key  UInt64,

    severity          UInt8,
    distance_mi       Float32,
    duration_minutes  Float32,

    weather_timestamp DateTime64(0, 'UTC'),
    temperature_f     Float32,
    wind_chill_f      Float32,
    humidity_pct      Float32,
    pressure_in       Float32,
    visibility_mi     Float32,
    wind_speed_mph    Float32,
    precipitation_in  Float32,
    wind_direction    String,
    weather_condition String,

    sunrise_sunset        String,
    civil_twilight        String,
    nautical_twilight     String,
    astronomical_twilight String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(                  -- tháng UTC từ time_sk
    toDateTime(time_sk * 3600, 'UTC')
)
ORDER BY (time_sk, location_key, accident_id);


-- 8. Fact Congestion
DROP TABLE IF EXISTS gold.fact_congestion;

CREATE TABLE gold.fact_congestion
(
    congestion_id    String,
    time_sk          UInt64,
    end_time_sk      UInt64,
    location_key     UInt64,
    airport_key      UInt64,
    city_climate_key UInt64,

    severity         UInt8,
    distance_mi      Float32,
    duration_minutes Float32,

    delay_from_typical_traffic_mins Float32,
    delay_from_free_flow_speed_mins Float32,

    congestion_speed String,
    description      String,

    weather_timestamp DateTime64(3, 'UTC'),
    temperature_f     Float32,
    wind_chill_f      Float32,
    humidity_pct      Float32,
    pressure_in       Float32,
    visibility_mi     Float32,
    wind_direction    String,
    wind_speed_mph    Float32,
    precipitation_in  Float32,
    weather_event     String,
    weather_condition String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(
    toDateTime(time_sk * 3600, 'UTC')
)
ORDER BY (time_sk, location_key, congestion_id);


-- 9. Fact Weather Events
DROP TABLE IF EXISTS gold.fact_weather_events;

CREATE TABLE gold.fact_weather_events
(
    weather_event_id String,
    time_sk          UInt64,
    end_time_sk      UInt64,
    location_key     UInt64,
    airport_key      UInt64,
    weather_type_key UInt64,
    city_climate_key UInt64,

    type             String,
    severity         String,
    precipitation    Float32,
    duration_minutes Float32,
    timezone         String,
    start_lat        Float64,
    start_lng        Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(
    toDateTime(time_sk * 3600, 'UTC')
)
ORDER BY (time_sk, location_key, weather_event_id);


-- =========================
-- GOLD: BRIDGE TABLES
-- =========================

-- 10. Bridge Accident x Weather Event
DROP TABLE IF EXISTS gold.bridge_accident_weather_event;

CREATE TABLE gold.bridge_accident_weather_event
(
    accident_id      String,
    weather_event_id String,
    overlap_minutes  Float32,
    distance_km      Float32
)
ENGINE = ReplacingMergeTree
ORDER BY (accident_id, weather_event_id);


-- 11. Bridge Accident x Congestion
DROP TABLE IF EXISTS gold.bridge_accident_congestion;

CREATE TABLE gold.bridge_accident_congestion
(
    accident_id      String,
    congestion_id    String,
    time_lag_minutes Float32,
    distance_km      Float32
)
ENGINE = ReplacingMergeTree
ORDER BY (accident_id, congestion_id);

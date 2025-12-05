INSERT INTO gold.fact_congestion (
    congestion_id,
    time_sk,
    end_time_sk,
    location_key,
    airport_key,
    city_climate_key,
    severity,
    distance_mi,
    duration_minutes,
    delay_from_typical_traffic_mins,
    delay_from_free_flow_speed_mins,
    congestion_speed,
    description,
    weather_timestamp,
    temperature_f,
    wind_chill_f,
    humidity_pct,
    pressure_in,
    visibility_mi,
    wind_direction,
    wind_speed_mph,
    precipitation_in,
    weather_event,
    weather_condition
)
SELECT
    s.ID AS congestion_id,

    -- time_sk từ Start_Time_UTC (giờ)
    toUInt64(intDiv(toUnixTimestamp(s.Start_Time_UTC), 3600)) AS time_sk,

    -- end_time_sk: 0 nếu NULL
    IF(
        s.End_Time_UTC IS NULL,
        0,
        toUInt64(intDiv(toUnixTimestamp(s.End_Time_UTC), 3600))
    ) AS end_time_sk,

    -- location_key
    cityHash64(
        coalesce(toString(s.Start_Lat), ''),
        coalesce(toString(s.Start_Lng), ''),
        coalesce(s.City, ''),
        coalesce(s.State, ''),
        coalesce(s.Zipcode, '')
    ) AS location_key,

    -- airport_key
    cityHash64(s.Airport_Code) AS airport_key,

    -- city_climate_key
    cityHash64(
        lowerUTF8(coalesce(s.City, '')),
        lowerUTF8(coalesce(s.State, ''))
    ) AS city_climate_key,

    s.Severity AS severity,
    CAST(s.Distance_mi AS Float32) AS distance_mi,

    -- duration (phút), 0 nếu không có End_Time_UTC
    IF(
        s.End_Time_UTC IS NULL,
        0,
        (toUnixTimestamp(s.End_Time_UTC) - toUnixTimestamp(s.Start_Time_UTC)) / 60.0
    ) AS duration_minutes,

    CAST(s.Delay_From_Typical_Traffic_mins  AS Float32)
        AS delay_from_typical_traffic_mins,
    CAST(s.Delay_From_Free_Flow_Speed_mins  AS Float32)
        AS delay_from_free_flow_speed_mins,

    s.Congestion_Speed,
    s.Description,

    s.Weather_Timestamp_UTC AS weather_timestamp,
    CAST(s.Temperature_F     AS Float32) AS temperature_f,
    CAST(s.Wind_Chill_F      AS Float32) AS wind_chill_f,
    CAST(s.Humidity_pct      AS Float32) AS humidity_pct,
    CAST(s.Pressure_in       AS Float32) AS pressure_in,
    CAST(s.Visibility_mi     AS Float32) AS visibility_mi,
    s.Wind_Direction,
    CAST(s.Wind_Speed_mph    AS Float32) AS wind_speed_mph,
    CAST(s.Precipitation_in  AS Float32) AS precipitation_in,
    coalesce(s.Weather_Event, '')        AS weather_event,
    s.Weather_Condition
FROM silver.traffic_congestion AS s;
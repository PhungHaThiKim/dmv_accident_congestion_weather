INSERT INTO gold.fact_weather_events (
    weather_event_id,
    time_sk,
    end_time_sk,
    location_key,
    airport_key,
    weather_type_key,
    city_climate_key,
    type,
    severity,
    precipitation,
    duration_minutes,
    timezone,
    start_lat,
    start_lng
)
SELECT
    s.Event_ID AS weather_event_id,

    -- time_sk từ StartTime_UTC
    toUInt64(intDiv(toUnixTimestamp(s.StartTime_UTC), 3600)) AS time_sk,

    -- end_time_sk: 0 nếu không có EndTime_UTC
    IF(
        s.EndTime_UTC IS NULL,
        0,
        toUInt64(intDiv(toUnixTimestamp(s.EndTime_UTC), 3600))
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
    cityHash64(s.AirportCode) AS airport_key,

    -- weather_type_key từ Type
    cityHash64(s.Type) AS weather_type_key,

    -- city_climate_key
    cityHash64(
        lowerUTF8(coalesce(s.City, '')),
        lowerUTF8(coalesce(s.State, ''))
    ) AS city_climate_key,

    s.Type,
    s.Severity,
    CAST(s.Precipitation AS Float32) AS precipitation,

    -- duration (phút), 0 nếu không có EndTime_UTC
    IF(
        s.EndTime_UTC IS NULL,
        0,
        (toUnixTimestamp(s.EndTime_UTC) - toUnixTimestamp(s.StartTime_UTC)) / 60.0
    ) AS duration_minutes,

    s.Timezone,
    s.Start_Lat,
    s.Start_Lng
FROM silver.weather_events AS s;
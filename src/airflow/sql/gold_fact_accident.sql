INSERT INTO gold.fact_accident (
    accident_id,
    time_sk,
    end_time_sk,
    location_key,
    airport_key,
    infra_key,
    city_climate_key,
    severity,
    distance_mi,
    duration_minutes,
    weather_timestamp,
    temperature_f,
    wind_chill_f,
    humidity_pct,
    pressure_in,
    visibility_mi,
    wind_speed_mph,
    precipitation_in,
    wind_direction,
    weather_condition,
    sunrise_sunset,
    civil_twilight,
    nautical_twilight,
    astronomical_twilight
)
SELECT
    s.ID AS accident_id,

    -- time_sk: giờ UTC từ Start_Time_UTC
    toUInt64(intDiv(toUnixTimestamp(s.Start_Time_UTC), 3600)) AS time_sk,
    toUInt64(intDiv(toUnixTimestamp(s.End_Time_UTC),   3600)) AS end_time_sk,

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

    -- infra_key: hash các flag hạ tầng
    cityHash64(
        toUInt8(s.Amenity),
        toUInt8(s.Bump),
        toUInt8(s.Crossing),
        toUInt8(s.Give_Way),
        toUInt8(s.Junction),
        toUInt8(s.No_Exit),
        toUInt8(s.Railway),
        toUInt8(s.Roundabout),
        toUInt8(s.Station),
        toUInt8(s.Stop),
        toUInt8(s.Traffic_Calming),
        toUInt8(s.Traffic_Signal),
        toUInt8(s.Turning_Loop)
    ) AS infra_key,

    -- city_climate_key
    cityHash64(
        lowerUTF8(coalesce(s.City, '')),
        lowerUTF8(coalesce(s.State, ''))
    ) AS city_climate_key,

    s.Severity AS severity,
    s.Distance_mi AS distance_mi,

    -- duration (phút) – End_Time_UTC ở silver là non-null
    (toUnixTimestamp(s.End_Time_UTC) - toUnixTimestamp(s.Start_Time_UTC)) / 60.0
        AS duration_minutes,

    s.Weather_Timestamp_UTC AS weather_timestamp,
    s.Temperature_F         AS temperature_f,
    s.Wind_Chill_F          AS wind_chill_f,
    s.Humidity_pct          AS humidity_pct,
    s.Pressure_in           AS pressure_in,
    s.Visibility_mi         AS visibility_mi,
    s.Wind_Speed_mph        AS wind_speed_mph,
    s.Precipitation_in      AS precipitation_in,
    s.Wind_Direction,
    s.Weather_Condition,

    s.Sunrise_Sunset,
    s.Civil_Twilight,
    s.Nautical_Twilight,
    s.Astronomical_Twilight
FROM silver.accidents AS s;
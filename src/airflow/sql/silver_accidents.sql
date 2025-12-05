INSERT INTO silver.accidents
WITH
    coalesce(nullIf(trim(Timezone), ''), 'UTC') AS tz,
    toString(Start_Time) AS st_str,
    toString(End_Time) AS et_str,
    toString(Weather_Timestamp) AS wt_str
SELECT
    ID,
    coalesce(nullIf(trim(Source), ''), 'Unknown') AS Source,
    coalesce(Severity, 0) AS Severity,

    -- local naive giữ nguyên từ bronze (đã là DateTime64)
    Start_Time AS Start_Time_Local,
    End_Time   AS End_Time_Local,

    /* Start_Time_UTC */
    multiIf(
        tz = 'US/Eastern',
            toTimeZone(parseDateTime64BestEffortOrNull(st_str, 0, 'US/Eastern'), 'UTC'),
        tz = 'US/Central',
            toTimeZone(parseDateTime64BestEffortOrNull(st_str, 0, 'US/Central'), 'UTC'),
        tz = 'US/Mountain',
            toTimeZone(parseDateTime64BestEffortOrNull(st_str, 0, 'US/Mountain'), 'UTC'),
        tz = 'US/Pacific',
            toTimeZone(parseDateTime64BestEffortOrNull(st_str, 0, 'US/Pacific'), 'UTC'),
            toTimeZone(parseDateTime64BestEffortOrNull(st_str, 0, 'UTC'), 'UTC')
    ) AS Start_Time_UTC,

    /* End_Time_UTC */
    multiIf(
        tz = 'US/Eastern',
            toTimeZone(parseDateTime64BestEffortOrNull(et_str, 0, 'US/Eastern'), 'UTC'),
        tz = 'US/Central',
            toTimeZone(parseDateTime64BestEffortOrNull(et_str, 0, 'US/Central'), 'UTC'),
        tz = 'US/Mountain',
            toTimeZone(parseDateTime64BestEffortOrNull(et_str, 0, 'US/Mountain'), 'UTC'),
        tz = 'US/Pacific',
            toTimeZone(parseDateTime64BestEffortOrNull(et_str, 0, 'US/Pacific'), 'UTC'),
            toTimeZone(parseDateTime64BestEffortOrNull(et_str, 0, 'UTC'), 'UTC')
    ) AS End_Time_UTC,

    coalesce(Start_Lat, 0.0) AS Start_Lat,
    coalesce(Start_Lng, 0.0) AS Start_Lng,
    End_Lat, End_Lng,

    `Distance(mi)` AS Distance_mi,

    coalesce(nullIf(trim(Description), ''), '') AS Description,
    coalesce(nullIf(trim(Street), ''), '') AS Street,

    coalesce(nullIf(trim(City), ''), 'Unknown') AS City,
    coalesce(nullIf(trim(County), ''), 'Unknown') AS County,
    coalesce(nullIf(trim(State), ''), 'Unknown') AS State,
    coalesce(nullIf(trim(Zipcode), ''), '') AS Zipcode,
    coalesce(nullIf(trim(Country), ''), 'US') AS Country,
    tz AS Timezone,
    coalesce(nullIf(trim(Airport_Code), ''), '') AS Airport_Code,

    Weather_Timestamp AS Weather_Timestamp_Local,

    /* Weather_Timestamp_UTC */
        multiIf(
            tz = 'US/Eastern',
                toTimeZone(parseDateTime64BestEffortOrNull(wt_str, 0, 'US/Eastern'), 'UTC'),
            tz = 'US/Central',
                toTimeZone(parseDateTime64BestEffortOrNull(wt_str, 0, 'US/Central'), 'UTC'),
            tz = 'US/Mountain',
                toTimeZone(parseDateTime64BestEffortOrNull(wt_str, 0, 'US/Mountain'), 'UTC'),
            tz = 'US/Pacific',
                toTimeZone(parseDateTime64BestEffortOrNull(wt_str, 0, 'US/Pacific'), 'UTC'),
                toTimeZone(parseDateTime64BestEffortOrNull(wt_str, 0, 'UTC'), 'UTC')
        ) AS Weather_Timestamp_UTC,

    `Temperature(F)` AS Temperature_F,
    `Wind_Chill(F)`  AS Wind_Chill_F,
    `Humidity(%)`    AS Humidity_pct,
    `Pressure(in)`   AS Pressure_in,
    `Visibility(mi)` AS Visibility_mi,
    coalesce(nullIf(trim(Wind_Direction), ''), 'Unknown') AS Wind_Direction,
    `Wind_Speed(mph)` AS Wind_Speed_mph,
    `Precipitation(in)` AS Precipitation_in,
    coalesce(nullIf(trim(Weather_Condition), ''), 'Unknown') AS Weather_Condition,

    coalesce(Amenity, 0),
    coalesce(Bump, 0),
    coalesce(Crossing, 0),
    coalesce(Give_Way, 0),
    coalesce(Junction, 0),
    coalesce(No_Exit, 0),
    coalesce(Railway, 0),
    coalesce(Roundabout, 0),
    coalesce(Station, 0),
    coalesce(Stop, 0),
    coalesce(Traffic_Calming, 0),
    coalesce(Traffic_Signal, 0),
    coalesce(Turning_Loop, 0),

    coalesce(nullIf(trim(Sunrise_Sunset), ''), 'Unknown'),
    coalesce(nullIf(trim(Civil_Twilight), ''), 'Unknown'),
    coalesce(nullIf(trim(Nautical_Twilight), ''), 'Unknown'),
    coalesce(nullIf(trim(Astronomical_Twilight), ''), 'Unknown'),

    now64(0) AS ingest_ts
FROM bronze.accidents
WHERE Start_Time IS NOT NULL
  AND Start_Time >= toDateTime64('{ds_start} 00:00:00', 0)
  AND Start_Time <  toDateTime64('{ds_end} 00:00:00', 0);


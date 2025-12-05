INSERT INTO traffic_congestion
WITH
    coalesce(nullIf(trim(LocalTimeZone), ''), 'UTC') AS tz,
    toString(StartTime)        AS st_str,
    toString(EndTime)          AS et_str,
    toString(WeatherTimeStamp) AS wt_str
SELECT
    -- keys / severity
    ID,
    coalesce(Severity, 0) AS Severity,

    -- location
    coalesce(Start_Lat, 0.0) AS Start_Lat,
    coalesce(Start_Lng, 0.0) AS Start_Lng,

    -- local naive time (source)
    StartTime AS Start_Time_Local,
    EndTime   AS End_Time_Local,

    /* Start_Time_UTC: Local -> UTC with constant tz literals */
    multiIf(
        tz = 'US/Eastern',
            toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(st_str), 0, 'US/Eastern'), 'UTC'),
        tz = 'US/Central',
            toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(st_str), 0, 'US/Central'), 'UTC'),
        tz = 'US/Mountain',
            toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(st_str), 0, 'US/Mountain'), 'UTC'),
        tz = 'US/Pacific',
            toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(st_str), 0, 'US/Pacific'), 'UTC'),
        -- fallback
            toDateTime64(parseDateTime64BestEffortOrNull(st_str), 0, 'UTC')
    ) AS Start_Time_UTC,

    /* End_Time_UTC */
    if(
        EndTime IS NULL OR trim(toString(EndTime)) = '',
        NULL,
        multiIf(
            tz = 'US/Eastern',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(et_str), 0, 'US/Eastern'), 'UTC'),
            tz = 'US/Central',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(et_str), 0, 'US/Central'), 'UTC'),
            tz = 'US/Mountain',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(et_str), 0, 'US/Mountain'), 'UTC'),
            tz = 'US/Pacific',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(et_str), 0, 'US/Pacific'), 'UTC'),
                toDateTime64(parseDateTime64BestEffortOrNull(et_str), 0, 'UTC')
        )
    ) AS End_Time_UTC,

    -- distance + delays
    `Distance(mi)` AS Distance_mi,
    `DelayFromTypicalTraffic(mins)` AS Delay_From_Typical_Traffic_mins,
    `DelayFromFreeFlowSpeed(mins)`  AS Delay_From_Free_Flow_Speed_mins,

    -- speed category/text
    coalesce(nullIf(trim(Congestion_Speed), ''), 'Unknown') AS Congestion_Speed,

    -- description + address
    coalesce(nullIf(trim(Description), ''), '') AS Description,
    coalesce(nullIf(trim(Street), ''), '') AS Street,
    coalesce(nullIf(trim(City), ''), 'Unknown') AS City,
    coalesce(nullIf(trim(County), ''), 'Unknown') AS County,
    coalesce(nullIf(trim(State), ''), 'Unknown') AS State,
    coalesce(nullIf(trim(ZipCode), ''), '') AS Zipcode,
    coalesce(nullIf(trim(Country), ''), 'US') AS Country,

    -- timezone + airport/station
    tz AS Timezone,
    coalesce(nullIf(trim(WeatherStation_AirportCode), ''), '') AS Airport_Code,

    -- weather timestamp local
    WeatherTimeStamp AS Weather_Timestamp_Local,

    /* Weather_Timestamp_UTC */
    if(
        WeatherTimeStamp IS NULL OR trim(toString(WeatherTimeStamp)) = '',
        NULL,
        multiIf(
            tz = 'US/Eastern',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(wt_str), 3, 'US/Eastern'), 'UTC'),
            tz = 'US/Central',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(wt_str), 3, 'US/Central'), 'UTC'),
            tz = 'US/Mountain',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(wt_str), 3, 'US/Mountain'), 'UTC'),
            tz = 'US/Pacific',
                toTimeZone(toDateTime64(parseDateTime64BestEffortOrNull(wt_str), 3, 'US/Pacific'), 'UTC'),
                toDateTime64(parseDateTime64BestEffortOrNull(wt_str), 3, 'UTC')
        )
    ) AS Weather_Timestamp_UTC,

    -- weather metrics
    `Temperature(F)` AS Temperature_F,
    `WindChill(F)`   AS Wind_Chill_F,
    `Humidity(%)`    AS Humidity_pct,
    `Pressure(in)`   AS Pressure_in,
    `Visibility(mi)` AS Visibility_mi,
    coalesce(nullIf(trim(WindDir), ''), 'Unknown') AS Wind_Direction,
    `WindSpeed(mph)` AS Wind_Speed_mph,
    `Precipitation(in)` AS Precipitation_in,

    -- weather type/condition
    Weather_Event,
    coalesce(nullIf(trim(Weather_Conditions), ''), 'Unknown') AS Weather_Condition,

    now64(0) AS ingest_ts

FROM bronze.congestion_events
WHERE
    -- dùng đúng tên cột StartTime (không phải Start_Time)
    StartTime IS NOT NULL
    AND parseDateTime64BestEffortOrNull(toString(StartTime)) >= toDateTime64('{ds_start} 00:00:00', 0)
    AND parseDateTime64BestEffortOrNull(toString(StartTime)) <  toDateTime64('{ds_end} 00:00:00', 0);

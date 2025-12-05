INSERT INTO silver.weather_events
WITH
    coalesce(nullIf(trim(TimeZone), ''), 'UTC') AS tz
SELECT
    -- IDs
    coalesce(nullIf(trim(EventId), ''), '') AS Event_ID,
    coalesce(nullIf(trim(AirportCode), ''), '') AS AirportCode,

    -- location
    coalesce(nullIf(trim(City), ''),   'Unknown') AS City,
    coalesce(nullIf(trim(County), ''), 'Unknown') AS County,
    coalesce(nullIf(trim(State), ''),  'Unknown') AS State,
    coalesce(nullIf(trim(ZipCode), ''), '')       AS Zipcode,

    /* StartTime_Local: UTC -> local theo TimeZone (must be constant literal) */
    multiIf(
        tz = 'US/Eastern',
            toTimeZone(`StartTime(UTC)`, 'US/Eastern'),
        tz = 'US/Central',
            toTimeZone(`StartTime(UTC)`, 'US/Central'),
        tz = 'US/Mountain',
            toTimeZone(`StartTime(UTC)`, 'US/Mountain'),
        tz = 'US/Pacific',
            toTimeZone(`StartTime(UTC)`, 'US/Pacific'),
        -- fallback: giữ nguyên UTC
            `StartTime(UTC)`
    ) AS StartTime_Local,

    /* EndTime_Local */
    if(
        `EndTime(UTC)` IS NULL,
        NULL,
        multiIf(
            tz = 'US/Eastern',
                toTimeZone(`EndTime(UTC)`, 'US/Eastern'),
            tz = 'US/Central',
                toTimeZone(`EndTime(UTC)`, 'US/Central'),
            tz = 'US/Mountain',
                toTimeZone(`EndTime(UTC)`, 'US/Mountain'),
            tz = 'US/Pacific',
                toTimeZone(`EndTime(UTC)`, 'US/Pacific'),
                `EndTime(UTC)`
        )
    ) AS EndTime_Local,

    -- UTC time: giữ nguyên từ bronze
    `StartTime(UTC)` AS StartTime_UTC,
    `EndTime(UTC)`   AS EndTime_UTC,
    tz               AS Timezone,

    -- type / severity / precip (giữ nguyên data type)
    coalesce(nullIf(trim(Type), ''), 'Unknown') AS Type,
    coalesce(nullIf(trim(Severity), ''), '')    AS Severity,
    coalesce(`Precipitation(in)`, 0.0)          AS Precipitation,

    -- lat / lng (Float64)
    coalesce(LocationLat, 0.0) AS Start_Lat,
    coalesce(LocationLng, 0.0) AS Start_Lng,

    now64(0, 'UTC') AS ingest_ts

FROM bronze.weather_events
WHERE
    `StartTime(UTC)` IS NOT NULL
    AND `StartTime(UTC)` >= toDateTime64('{ds_start} 00:00:00', 0, 'UTC')
    AND `StartTime(UTC)` <  toDateTime64('{ds_end} 00:00:00', 0, 'UTC');

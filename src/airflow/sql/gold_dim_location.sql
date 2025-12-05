INSERT INTO gold.dim_location (
    location_key,
    start_lat,
    start_lng,
    city,
    county,
    state,
    zipcode,
    country,
    region,
    is_urban
)
SELECT DISTINCT
    cityHash64(
        coalesce(toString(start_lat), ''),
        coalesce(toString(start_lng), ''),
        coalesce(city, ''),
        coalesce(state, ''),
        coalesce(zipcode, '')
    ) AS location_key,
    start_lat,
    start_lng,
    city,
    county,
    state,
    zipcode,
    country,
    ''  AS region,
    0   AS is_urban
FROM
(
    SELECT
        Start_Lat AS start_lat,
        Start_Lng AS start_lng,
        City      AS city,
        County    AS county,
        State     AS state,
        Zipcode   AS zipcode,
        Country   AS country
    FROM silver.accidents

    UNION ALL

    SELECT
        Start_Lat AS start_lat,
        Start_Lng AS start_lng,
        City      AS city,
        County    AS county,
        State     AS state,
        Zipcode   AS zipcode,
        Country   AS country
    FROM silver.traffic_congestion

    UNION ALL

    SELECT
        Start_Lat AS start_lat,
        Start_Lng AS start_lng,
        City      AS city,
        County    AS county,
        State     AS state,
        Zipcode   AS zipcode,
        'US'      AS country
    FROM silver.weather_events
) AS s
WHERE start_lat IS NOT NULL
  AND start_lng IS NOT NULL;
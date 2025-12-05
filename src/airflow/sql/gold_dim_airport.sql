INSERT INTO gold.dim_airport (
    airport_key,
    airport_code,
    name,
    city,
    state,
    country,
    latitude,
    longitude,
    timezone
)
SELECT DISTINCT
    cityHash64(airport_code) AS airport_key,
    airport_code,
    ''          AS name,
    ''          AS city,
    ''          AS state,
    'US'        AS country,
    0.0         AS latitude,
    0.0         AS longitude,
    ''          AS timezone
FROM
(
    SELECT Airport_Code AS airport_code
    FROM silver.accidents
    WHERE Airport_Code != ''

    UNION ALL

    SELECT Airport_Code AS airport_code
    FROM silver.traffic_congestion
    WHERE Airport_Code != ''

    UNION ALL

    SELECT AirportCode AS airport_code
    FROM silver.weather_events
    WHERE AirportCode != ''
) a;

INSERT INTO gold.dim_weather_type (
    weather_type_key,
    weather_type,
    weather_group,
    is_severe,
    description
)
SELECT DISTINCT
    cityHash64(weather_type) AS weather_type_key,
    weather_type,
    multiIf(
        lower(weather_type) LIKE '%rain%',  'Wet',
        lower(weather_type) LIKE '%snow%',  'Wintry',
        lower(weather_type) LIKE '%fog%',   'Foggy',
        lower(weather_type) LIKE '%storm%', 'Stormy',
        'Other'
    ) AS weather_group,
    (lower(weather_type) LIKE '%storm%' OR lower(weather_type) LIKE '%heavy%') AS is_severe,
    '' AS description
FROM
(
    SELECT Type AS weather_type
    FROM silver.weather_events

    UNION ALL

    SELECT coalesce(Weather_Event, Weather_Condition) AS weather_type
    FROM silver.traffic_congestion

    UNION ALL

    SELECT Weather_Condition AS weather_type
    FROM silver.accidents
) t
WHERE weather_type IS NOT NULL AND weather_type != '';

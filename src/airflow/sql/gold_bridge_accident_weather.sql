-- 1) Cùng hour: fa.time_sk = fw.time_sk
INSERT INTO gold.bridge_accident_weather_event (
    accident_id,
    weather_event_id,
    overlap_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fw.weather_event_id,
    0.0 AS overlap_minutes,   -- cùng hour, ta coi chênh = 0 phút
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_weather_events fw
    ON fa.city_climate_key = fw.city_climate_key
   AND fa.time_sk         = fw.time_sk;


-- 2) Weather sớm hơn 1h: fa.time_sk = fw.time_sk + 1
INSERT INTO gold.bridge_accident_weather_event (
    accident_id,
    weather_event_id,
    overlap_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fw.weather_event_id,
    60.0 AS overlap_minutes,  -- lệch đúng 1h
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_weather_events fw
    ON fa.city_climate_key = fw.city_climate_key
   AND fa.time_sk         = fw.time_sk + 1;


-- 3) Weather muộn hơn 1h: fa.time_sk + 1 = fw.time_sk
INSERT INTO gold.bridge_accident_weather_event (
    accident_id,
    weather_event_id,
    overlap_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fw.weather_event_id,
    60.0 AS overlap_minutes,  -- lệch đúng 1h
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_weather_events fw
    ON fa.city_climate_key = fw.city_climate_key
   AND fa.time_sk + 1      = fw.time_sk;
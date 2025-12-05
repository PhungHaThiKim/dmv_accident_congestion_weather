-- 1) Cùng hour: fa.time_sk = fc.time_sk
INSERT INTO gold.bridge_accident_congestion (
    accident_id,
    congestion_id,
    time_lag_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fc.congestion_id,
    0.0 AS time_lag_minutes,   -- cùng hour, coi như 0 phút lệch
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_congestion fc
    ON fa.city_climate_key = fc.city_climate_key
   AND fa.time_sk         = fc.time_sk;


-- 2) Congestion sớm hơn 1h: fa.time_sk = fc.time_sk + 1
INSERT INTO gold.bridge_accident_congestion (
    accident_id,
    congestion_id,
    time_lag_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fc.congestion_id,
    60.0 AS time_lag_minutes,  -- lệch đúng 1h
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_congestion fc
    ON fa.city_climate_key = fc.city_climate_key
   AND fa.time_sk         = fc.time_sk + 1;


-- 3) Congestion muộn hơn 1h: fa.time_sk + 1 = fc.time_sk
INSERT INTO gold.bridge_accident_congestion (
    accident_id,
    congestion_id,
    time_lag_minutes,
    distance_km
)
SELECT
    fa.accident_id,
    fc.congestion_id,
    60.0 AS time_lag_minutes,  -- lệch đúng 1h
    0.0 AS distance_km
FROM gold.fact_accident fa
ANY INNER JOIN gold.fact_congestion fc
    ON fa.city_climate_key = fc.city_climate_key
   AND fa.time_sk + 1      = fc.time_sk;
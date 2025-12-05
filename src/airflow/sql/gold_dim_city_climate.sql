INSERT INTO gold.dim_city_climate (
    city_climate_key,
    city,
    state,
    climate_zone,
    has_snow_winter,
    has_wet_dry_pattern,
    notes
)
VALUES
(
    cityHash64('New York','NY'),
    'New York', 'NY',
    'Humid_Subtropical',
    1, 0,
    'Cold snowy winters, hot humid summers'
),
(
    cityHash64('Chicago','IL'),
    'Chicago', 'IL',
    'Humid_Continental',
    1, 0,
    'Very cold winters, hot summers'
),
(
    cityHash64('Miami','FL'),
    'Miami', 'FL',
    'Tropical_Monsoon',
    0, 1,
    'Wet vs dry season, no real winter'
),
(
    cityHash64('Los Angeles','CA'),
    'Los Angeles', 'CA',
    'Mediterranean',
    0, 1,
    'Wet winter, dry summer'
);

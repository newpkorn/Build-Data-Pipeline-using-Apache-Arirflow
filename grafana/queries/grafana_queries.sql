-- Grafana Query Snippets
-- Purpose:
--   This file is a reference library of example SQL snippets for Grafana panels.
-- Important:
--   Use one query block at a time in a Grafana panel.
--   Do not paste this entire file into a single panel query editor.


/* ========================================================================== */
/* FX: USD THB Trend                                                          */
/* ========================================================================== */
SELECT
  UNIX_TIMESTAMP(rate_date) AS time_sec,
  rate AS value,
  'USD' AS metric
FROM global_exchange_rates
WHERE currency_code = 'USD'
ORDER BY rate_date ASC;


/* ========================================================================== */
/* FX: EUR THB Trend                                                          */
/* ========================================================================== */
SELECT
  UNIX_TIMESTAMP(rate_date) AS time_sec,
  rate AS value,
  'EUR' AS metric
FROM global_exchange_rates
WHERE currency_code = 'EUR'
ORDER BY rate_date ASC;


/* ========================================================================== */
/* FX: Top 10 Latest Rates                                                    */
/* ========================================================================== */
SELECT
  currency_code AS metric,
  rate AS value
FROM global_exchange_rates
WHERE rate_date = (
  SELECT MAX(rate_date)
  FROM global_exchange_rates
)
ORDER BY rate DESC
LIMIT 10;


/* ========================================================================== */
/* Weather: Bangkok Temperature Trend                                         */
/* ========================================================================== */
SELECT
  observed_at_local AS time,
  temperature_celsius AS value,
  'Temperature' AS metric
FROM weather_observations
WHERE city = 'Bangkok'
ORDER BY observed_at_local ASC;


/* ========================================================================== */
/* Weather: Bangkok Latest Snapshot                                           */
/* ========================================================================== */
SELECT
  city,
  weather_description,
  temperature_celsius,
  feels_like_celsius,
  humidity,
  wind_speed,
  pressure,
  observed_at_local
FROM weather_observations
WHERE city = 'Bangkok'
ORDER BY observed_at_local DESC
LIMIT 1;

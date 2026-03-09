-- USD THB Trend

SELECT
UNIX_TIMESTAMP(rate_date) as time_sec,
rate as value,
'USD' as metric
FROM global_exchange_rates
WHERE currency_code = 'USD'
ORDER BY rate_date ASC;


-- EUR THB Trend

SELECT
UNIX_TIMESTAMP(rate_date) as time_sec,
rate as value,
'EUR' as metric
FROM global_exchange_rates
WHERE currency_code = 'EUR'
ORDER BY rate_date ASC;


-- Top FX Rates

SELECT
currency_code as metric,
rate as value
FROM global_exchange_rates
WHERE rate_date = (SELECT MAX(rate_date) FROM global_exchange_rates)
ORDER BY rate DESC
LIMIT 10;
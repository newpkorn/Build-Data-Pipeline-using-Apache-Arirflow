-- USD THB Trend

SELECT
rate_date as time,
rate
FROM bot_exchange_rates
WHERE currency='USD'
ORDER BY rate_date;


-- EUR THB Trend

SELECT
rate_date as time,
rate
FROM bot_exchange_rates
WHERE currency='EUR'
ORDER BY rate_date;


-- Top FX Rates

SELECT
currency,
rate
FROM bot_exchange_rates
ORDER BY rate DESC
LIMIT 10;
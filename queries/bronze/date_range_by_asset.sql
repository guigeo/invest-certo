SELECT
    asset,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    DATE_DIFF('day', MIN(date), MAX(date)) AS span_days
FROM bronze_prices
GROUP BY asset
ORDER BY asset;

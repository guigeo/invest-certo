SELECT
    asset,
    ticker,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    COUNT(*) AS total_rows
FROM bronze_prices
GROUP BY asset, ticker
ORDER BY asset;

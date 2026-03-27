SELECT
    date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    asset,
    ticker
FROM bronze_prices
WHERE asset = 'BBAS3'
ORDER BY date;

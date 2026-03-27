SELECT
    asset,
    ticker,
    asset_type,
    MIN(date) AS min_date,
    MAX(date) AS max_date,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN is_volume_missing THEN 1 ELSE 0 END) AS volume_missing_rows
FROM read_parquet('data/silver/prices_clean/**/*.parquet')
GROUP BY asset, ticker, asset_type
ORDER BY asset;

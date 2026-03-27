SELECT
    date,
    eligibility_status,
    COUNT(*) AS asset_count
FROM read_parquet('data/silver/asset_daily_status/**/*.parquet')
GROUP BY date, eligibility_status
ORDER BY date DESC, eligibility_status;

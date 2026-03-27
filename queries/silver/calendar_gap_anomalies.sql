SELECT
    asset,
    ticker,
    asset_type,
    date,
    prev_date,
    days_since_prev_trade,
    eligibility_status
FROM read_parquet('data/silver/asset_daily_status/**/*.parquet')
WHERE has_calendar_gap_anomaly
ORDER BY date DESC, asset;

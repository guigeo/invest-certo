WITH latest_status AS (
    SELECT
        asset,
        ticker,
        asset_type,
        date,
        history_length_days,
        is_zero_volume,
        is_volume_missing,
        is_feature_eligible,
        eligibility_status,
        ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS row_num
    FROM read_parquet('data/silver/asset_daily_status/**/*.parquet')
)
SELECT
    asset,
    ticker,
    asset_type,
    date,
    history_length_days,
    is_zero_volume,
    is_volume_missing,
    is_feature_eligible,
    eligibility_status
FROM latest_status
WHERE row_num = 1
  AND is_feature_eligible
ORDER BY asset;

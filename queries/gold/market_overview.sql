WITH features AS (
    SELECT *
    FROM read_parquet('data/gold/asset_features/**/*.parquet')
),
ranking AS (
    SELECT *
    FROM read_parquet('data/gold/ranking_snapshot/**/*.parquet')
)
SELECT
    r.reference_date,
    SUM(CASE WHEN r.eligibility_status = 'eligible' THEN 1 ELSE 0 END) AS eligible_asset_count,
    SUM(CASE WHEN r.ranking_bucket = 'top_3' THEN 1 ELSE 0 END) AS top_3_count,
    SUM(CASE WHEN r.ranking_bucket = 'top_5' THEN 1 ELSE 0 END) AS top_5_count,
    SUM(CASE WHEN r.ranking_bucket = 'middle' THEN 1 ELSE 0 END) AS middle_count,
    SUM(CASE WHEN r.ranking_bucket = 'tail' THEN 1 ELSE 0 END) AS tail_count,
    AVG(f.return_90d) AS avg_return_90d,
    MEDIAN(f.return_90d) AS median_return_90d,
    AVG(f.volatility_30d) AS avg_volatility_30d,
    MEDIAN(f.volatility_30d) AS median_volatility_30d,
    SUM(CASE WHEN f.trend_ratio > 1 THEN 1 ELSE 0 END) AS positive_trend_count,
    SUM(CASE WHEN f.price_vs_52w_high >= -0.05 THEN 1 ELSE 0 END) AS near_52w_high_count,
    SUM(CASE WHEN f.price_vs_52w_low <= 0.05 THEN 1 ELSE 0 END) AS near_52w_low_count
FROM ranking AS r
JOIN features AS f
  ON r.reference_date = f.reference_date
 AND r.asset = f.asset
GROUP BY r.reference_date
ORDER BY r.reference_date;

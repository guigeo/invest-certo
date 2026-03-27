WITH latest_date AS (
    SELECT MAX(reference_date) AS reference_date
    FROM read_parquet('data/gold/ranking_snapshot/**/*.parquet')
)
SELECT
    r.reference_date,
    r.asset,
    r.ticker,
    r.asset_type,
    r.rank_position,
    r.score,
    r.ranking_bucket,
    r.rank_delta_7d,
    r.rank_delta_30d,
    r.score_delta_7d,
    r.score_delta_30d,
    r.primary_signal,
    r.secondary_signal,
    r.is_top_pick,
    r.eligibility_status,
    f.return_90d,
    f.volatility_30d,
    f.drawdown_252d,
    f.trend_ratio,
    f.distance_to_ma20,
    f.distance_to_ma90,
    f.price_vs_52w_high,
    f.price_vs_52w_low,
    f.momentum_bucket,
    f.risk_bucket
FROM read_parquet('data/gold/ranking_snapshot/**/*.parquet') AS r
JOIN read_parquet('data/gold/asset_features/**/*.parquet') AS f
  ON r.reference_date = f.reference_date
 AND r.asset = f.asset
JOIN latest_date AS d
  ON r.reference_date = d.reference_date
ORDER BY r.rank_position, r.asset;

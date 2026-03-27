SELECT
    reference_date,
    asset,
    ticker,
    asset_type,
    rank_position,
    score,
    rank_delta_7d,
    rank_delta_30d,
    score_delta_7d,
    score_delta_30d,
    ranking_bucket,
    eligibility_status,
    is_top_pick
FROM read_parquet('data/gold/ranking_snapshot/**/*.parquet')
ORDER BY asset, reference_date;

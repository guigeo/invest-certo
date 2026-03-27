SELECT
    reference_date,
    asset,
    ticker,
    asset_type,
    close,
    adj_close,
    ma_20,
    ma_90,
    drawdown_252d,
    volatility_30d,
    return_30d,
    return_90d,
    return_252d,
    distance_to_ma20,
    distance_to_ma90,
    price_vs_52w_high,
    price_vs_52w_low,
    momentum_bucket,
    risk_bucket
FROM read_parquet('data/gold/asset_features/**/*.parquet')
ORDER BY asset, reference_date;

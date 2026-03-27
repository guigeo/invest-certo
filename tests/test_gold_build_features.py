from pathlib import Path

import duckdb
import pandas as pd

from pipelines.gold.build_features import (
    build_asset_features,
    build_ranking_snapshot,
    main,
)


def create_prices_clean(periods: int = 320) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=periods)
    rows = []
    asset_specs = [
        ("ALFA3", 10.0, 0.18, None),
        ("BETA3", 20.0, 0.05, dates[-1]),
        ("CURTA3", 30.0, 0.03, None),
    ]

    for asset, base_price, daily_step, missing_volume_date in asset_specs:
        asset_dates = dates if asset != "CURTA3" else dates[:180]
        for idx, date in enumerate(asset_dates):
            price = base_price + idx * daily_step
            rows.append(
                {
                    "date": date,
                    "asset": asset,
                    "ticker": f"{asset}.SA",
                    "asset_type": "stock",
                    "source": "yahoo",
                    "open": price * 0.99,
                    "high": price * 1.01,
                    "low": price * 0.98,
                    "close": price,
                    "adj_close": price,
                    "volume": 0 if date == missing_volume_date else 1000,
                    "is_volume_missing": date == missing_volume_date,
                }
            )
    return pd.DataFrame(rows)


def create_asset_daily_status(prices_clean: pd.DataFrame) -> pd.DataFrame:
    status = prices_clean[
        ["date", "asset", "ticker", "asset_type", "is_volume_missing"]
    ].copy()
    status = status.sort_values(["asset", "date"]).reset_index(drop=True)
    status["is_price_valid"] = True
    status["is_zero_volume"] = status["is_volume_missing"]
    status["prev_date"] = status.groupby("asset")["date"].shift(1)
    status["days_since_prev_trade"] = (
        status["date"] - status["prev_date"]
    ).dt.days
    status["has_calendar_gap_anomaly"] = False
    status["history_length_days"] = status.groupby("asset").cumcount() + 1
    status["has_min_history_30d"] = status["history_length_days"] >= 30
    status["has_min_history_90d"] = status["history_length_days"] >= 90
    status["has_min_history_252d"] = status["history_length_days"] >= 252
    status["eligibility_status"] = "eligible"
    status.loc[~status["has_min_history_252d"], "eligibility_status"] = "insufficient_history"
    status.loc[status["is_volume_missing"], "eligibility_status"] = "volume_missing"
    status["is_feature_eligible"] = status["eligibility_status"] == "eligible"
    return status


def write_dataset(df: pd.DataFrame, base_path: Path, date_column: str) -> None:
    for (year, month), partition_df in df.groupby(
        [pd.to_datetime(df[date_column]).dt.year, pd.to_datetime(df[date_column]).dt.month]
    ):
        partition_dir = base_path / f"reference_year={year}" / f"reference_month={month:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_df.to_parquet(partition_dir / "part.parquet", index=False)


def test_gold_builds_features_and_ranking_for_dashboard() -> None:
    prices_clean = create_prices_clean()
    asset_daily_status = create_asset_daily_status(prices_clean)

    asset_features = build_asset_features(prices_clean, asset_daily_status)
    ranking_snapshot = build_ranking_snapshot(asset_features, asset_daily_status)

    latest_date = ranking_snapshot["reference_date"].max()
    latest_snapshot = ranking_snapshot[ranking_snapshot["reference_date"] == latest_date]

    assert len(asset_features) == len(prices_clean)
    assert latest_snapshot.iloc[0]["asset"] == "ALFA3"
    assert bool(latest_snapshot.iloc[0]["is_top_pick"]) is True
    assert (
        latest_snapshot.loc[latest_snapshot["asset"] == "BETA3", "eligibility_status"].iloc[0]
        == "volume_missing"
    )
    assert (
        asset_features.loc[
            asset_features["asset"] == "CURTA3", "feature_status"
        ].iloc[-1]
        == "insufficient_history"
    )


def test_gold_populates_temporal_deltas_after_comparable_history() -> None:
    prices_clean = create_prices_clean()
    asset_daily_status = create_asset_daily_status(prices_clean)

    asset_features = build_asset_features(prices_clean, asset_daily_status)
    ranking_snapshot = build_ranking_snapshot(asset_features, asset_daily_status)
    alfa = ranking_snapshot[ranking_snapshot["asset"] == "ALFA3"].sort_values("reference_date")

    assert pd.isna(alfa.iloc[0]["rank_delta_7d"])
    assert pd.isna(alfa.iloc[0]["score_delta_30d"])
    assert pd.notna(alfa.iloc[-1]["rank_delta_7d"])
    assert pd.notna(alfa.iloc[-1]["score_delta_30d"])


def test_gold_latest_recommendations_query_returns_only_latest_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_clean = create_prices_clean()
    asset_daily_status = create_asset_daily_status(prices_clean)

    silver_prices_dir = tmp_path / "data" / "silver" / "prices_clean"
    silver_status_dir = tmp_path / "data" / "silver" / "asset_daily_status"
    write_dataset(prices_clean, silver_prices_dir, "date")
    write_dataset(asset_daily_status, silver_status_dir, "date")

    monkeypatch.chdir(tmp_path)
    exit_code = main()
    assert exit_code == 0

    query_path = Path(__file__).resolve().parents[1] / "queries" / "gold" / "latest_recommendations.sql"
    sql = query_path.read_text(encoding="utf-8")

    con = duckdb.connect(database=":memory:")
    try:
        result = con.execute(sql).fetchdf()
    finally:
        con.close()

    assert result["reference_date"].nunique() == 1
    assert result.iloc[0]["asset"] == "ALFA3"

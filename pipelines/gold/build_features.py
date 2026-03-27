from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import math
import shutil
import sys

import duckdb
import numpy as np
import pandas as pd

from src.validators.gold_validator import (
    GoldValidationError,
    validate_asset_features,
    validate_ranking_snapshot,
)


SILVER_BASE_PATH = Path("data/silver")
PRICES_CLEAN_PATH = SILVER_BASE_PATH / "prices_clean"
ASSET_DAILY_STATUS_PATH = SILVER_BASE_PATH / "asset_daily_status"
GOLD_BASE_PATH = Path("data/gold")
ASSET_FEATURES_OUTPUT = GOLD_BASE_PATH / "asset_features"
RANKING_SNAPSHOT_OUTPUT = GOLD_BASE_PATH / "ranking_snapshot"

ASSET_FEATURES_COLUMNS = [
    "reference_date",
    "asset",
    "ticker",
    "asset_type",
    "close",
    "adj_close",
    "daily_return",
    "return_30d",
    "return_90d",
    "return_252d",
    "volatility_30d",
    "drawdown_252d",
    "ma_20",
    "ma_90",
    "trend_ratio",
    "sharpe_like_90d",
    "data_points_252d",
    "feature_status",
    "distance_to_ma20",
    "distance_to_ma90",
    "price_vs_52w_high",
    "price_vs_52w_low",
    "momentum_bucket",
    "risk_bucket",
]

RANKING_SNAPSHOT_COLUMNS = [
    "reference_date",
    "asset",
    "ticker",
    "asset_type",
    "score",
    "rank_position",
    "ranking_bucket",
    "eligibility_status",
    "score_version",
    "has_complete_features",
    "rank_delta_7d",
    "rank_delta_30d",
    "score_delta_7d",
    "score_delta_30d",
    "primary_signal",
    "secondary_signal",
    "is_top_pick",
]

SCORE_VERSION = "v1"
SQRT_252 = math.sqrt(252.0)


def load_silver_dataset(dataset_path: Path) -> pd.DataFrame:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Diretorio Silver nao encontrado: {dataset_path}")

    parquet_files = list(dataset_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum parquet encontrado em: {dataset_path}")

    con = duckdb.connect(database=":memory:")
    try:
        dataset_glob = str(dataset_path / "**/*.parquet").replace("'", "''")
        return con.execute(f"SELECT * FROM read_parquet('{dataset_glob}')").fetchdf()
    finally:
        con.close()


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    result = numerator.astype("float64") / denominator.astype("float64")
    result = result.replace([np.inf, -np.inf], np.nan)
    return result


def _momentum_bucket(return_90d: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [return_90d >= 0.15, return_90d <= -0.05],
            ["strong", "weak"],
            default="neutral",
        ),
        index=return_90d.index,
    )


def _risk_bucket(volatility_30d: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [volatility_30d <= 0.20, volatility_30d <= 0.35],
            ["low", "medium"],
            default="high",
        ),
        index=volatility_30d.index,
    )


def _feature_status(row: pd.Series) -> str:
    if row["eligibility_status"] != "eligible":
        return str(row["eligibility_status"])
    if row["data_points_252d"] < 252:
        return "insufficient_history"
    required_feature_columns = [
        "return_90d",
        "volatility_30d",
        "drawdown_252d",
        "ma_20",
        "ma_90",
        "trend_ratio",
        "sharpe_like_90d",
    ]
    if row[required_feature_columns].isnull().any():
        return "insufficient_history"
    return "complete"


def build_asset_features(
    prices_clean: pd.DataFrame,
    asset_daily_status: pd.DataFrame,
) -> pd.DataFrame:
    prices_df = prices_clean.copy()
    status_df = asset_daily_status.copy()

    prices_df["date"] = pd.to_datetime(prices_df["date"])
    status_df["date"] = pd.to_datetime(status_df["date"])

    merged = prices_df.merge(
        status_df[
            [
                "date",
                "asset",
                "eligibility_status",
                "is_feature_eligible",
            ]
        ],
        on=["date", "asset"],
        how="inner",
        validate="one_to_one",
    )
    merged = merged.sort_values(["asset", "date"]).reset_index(drop=True)

    feature_frames: list[pd.DataFrame] = []
    for _, asset_df in merged.groupby("asset", sort=True):
        asset_df = asset_df.sort_values("date").copy()
        adjusted = asset_df["adj_close"].astype("float64")
        daily_return = adjusted.pct_change()
        ma_20 = adjusted.rolling(window=20, min_periods=20).mean()
        ma_90 = adjusted.rolling(window=90, min_periods=90).mean()
        rolling_max_252 = adjusted.rolling(window=252, min_periods=1).max()
        rolling_min_252 = adjusted.rolling(window=252, min_periods=1).min()
        rolling_std_30 = daily_return.rolling(window=30, min_periods=30).std(ddof=0)
        rolling_std_90 = daily_return.rolling(window=90, min_periods=90).std(ddof=0)
        rolling_mean_90 = daily_return.rolling(window=90, min_periods=90).mean()

        asset_df["daily_return"] = daily_return
        asset_df["return_30d"] = adjusted.div(adjusted.shift(30)).sub(1.0)
        asset_df["return_90d"] = adjusted.div(adjusted.shift(90)).sub(1.0)
        asset_df["return_252d"] = adjusted.div(adjusted.shift(252)).sub(1.0)
        asset_df["volatility_30d"] = rolling_std_30 * SQRT_252
        asset_df["drawdown_252d"] = adjusted.div(rolling_max_252).sub(1.0)
        asset_df["ma_20"] = ma_20
        asset_df["ma_90"] = ma_90
        asset_df["trend_ratio"] = _safe_ratio(ma_20, ma_90)
        asset_df["sharpe_like_90d"] = (
            _safe_ratio(rolling_mean_90, rolling_std_90) * SQRT_252
        )
        asset_df["data_points_252d"] = (
            adjusted.rolling(window=252, min_periods=1).count().astype(int)
        )
        asset_df["distance_to_ma20"] = adjusted.div(ma_20).sub(1.0)
        asset_df["distance_to_ma90"] = adjusted.div(ma_90).sub(1.0)
        asset_df["price_vs_52w_high"] = adjusted.div(rolling_max_252).sub(1.0)
        asset_df["price_vs_52w_low"] = adjusted.div(rolling_min_252).sub(1.0)

        feature_frames.append(asset_df)

    features = pd.concat(feature_frames, ignore_index=True)
    features["momentum_bucket"] = _momentum_bucket(features["return_90d"])
    features["risk_bucket"] = _risk_bucket(features["volatility_30d"])
    features["feature_status"] = features.apply(_feature_status, axis=1)

    features = features.rename(columns={"date": "reference_date"})
    features = features[ASSET_FEATURES_COLUMNS].copy()
    validate_asset_features(features)
    return features


def _score_eligible_assets(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")

    trend_score = df["trend_ratio"].rank(pct=True, method="average")
    momentum_score = df["return_90d"].rank(pct=True, method="average")
    sharpe_score = df["sharpe_like_90d"].rank(pct=True, method="average")
    volatility_penalty = 1.0 - df["volatility_30d"].rank(pct=True, method="average")
    drawdown_penalty = 1.0 - df["drawdown_252d"].rank(pct=True, method="average")

    return (
        0.30 * trend_score
        + 0.30 * momentum_score
        + 0.25 * sharpe_score
        + 0.10 * volatility_penalty
        + 0.05 * drawdown_penalty
    )


def _ranking_bucket(rank_position: int, eligible_count: int) -> str:
    if rank_position <= 3:
        return "top_3"
    if rank_position <= 5:
        return "top_5"
    if rank_position <= max(eligible_count, 1):
        return "middle"
    return "tail"


def _primary_signal(row: pd.Series) -> str:
    if row["trend_ratio"] >= 1.02 and row["return_90d"] >= 0.05:
        return "trend_strength"
    if row["sharpe_like_90d"] >= 1.0:
        return "risk_adjusted_quality"
    if row["price_vs_52w_high"] <= -0.15 and row["return_30d"] > 0:
        return "recovery_setup"
    return "balanced_setup"


def _secondary_signal(row: pd.Series) -> str:
    if row["risk_bucket"] == "low":
        return "risk_control"
    if row["distance_to_ma20"] > 0 and row["distance_to_ma90"] > 0:
        return "price_above_moving_averages"
    if row["drawdown_252d"] <= -0.20:
        return "deep_drawdown"
    return "watchlist"


def _attach_history_deltas(
    ranking_snapshot: pd.DataFrame,
    days: int,
) -> pd.DataFrame:
    result_frames: list[pd.DataFrame] = []
    for _, asset_df in ranking_snapshot.groupby("asset", sort=True):
        asset_df = asset_df.sort_values("reference_date").copy()
        asset_df["reference_date"] = pd.to_datetime(
            asset_df["reference_date"]
        ).astype("datetime64[ns]")
        history = asset_df[["reference_date", "rank_position", "score"]].copy()
        history = history.rename(
            columns={
                "reference_date": "history_date",
                "rank_position": f"prior_rank_{days}d",
                "score": f"prior_score_{days}d",
            }
        )
        history["history_date"] = pd.to_datetime(history["history_date"]).astype(
            "datetime64[ns]"
        )
        history["available_from"] = (
            history["history_date"] + pd.Timedelta(days=days)
        ).astype("datetime64[ns]")

        merged = pd.merge_asof(
            asset_df.sort_values("reference_date"),
            history.sort_values("available_from"),
            left_on="reference_date",
            right_on="available_from",
            direction="backward",
        )

        merged[f"rank_delta_{days}d"] = (
            merged[f"prior_rank_{days}d"] - merged["rank_position"]
        )
        merged[f"score_delta_{days}d"] = (
            merged["score"] - merged[f"prior_score_{days}d"]
        )
        result_frames.append(merged.drop(columns=["history_date", "available_from"]))

    return pd.concat(result_frames, ignore_index=True)


def build_ranking_snapshot(
    asset_features: pd.DataFrame,
    asset_daily_status: pd.DataFrame,
) -> pd.DataFrame:
    status_df = asset_daily_status.rename(columns={"date": "reference_date"}).copy()
    status_df["reference_date"] = pd.to_datetime(status_df["reference_date"])

    ranking = asset_features.merge(
        status_df[["reference_date", "asset", "eligibility_status", "is_feature_eligible"]],
        on=["reference_date", "asset"],
        how="inner",
        validate="one_to_one",
    )
    ranking["reference_date"] = pd.to_datetime(ranking["reference_date"])
    ranking["has_complete_features"] = ranking["feature_status"] == "complete"
    ranking["score"] = 0.0
    ranking["score_version"] = SCORE_VERSION

    ranked_frames: list[pd.DataFrame] = []
    for reference_date, date_df in ranking.groupby("reference_date", sort=True):
        date_df = date_df.copy()
        eligible_mask = (
            date_df["is_feature_eligible"] & date_df["has_complete_features"]
        )

        eligible = date_df.loc[eligible_mask].copy()
        date_df.loc[eligible_mask, "score"] = _score_eligible_assets(eligible).values

        eligible_count = int(eligible_mask.sum())
        date_df = date_df.sort_values(
            by=["is_feature_eligible", "has_complete_features", "score", "asset"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        date_df["rank_position"] = np.arange(1, len(date_df) + 1)
        eligible_position_mask = (
            date_df["is_feature_eligible"] & date_df["has_complete_features"]
        )
        eligible_rank = np.where(
            eligible_position_mask,
            eligible_position_mask.cumsum(),
            np.nan,
        )
        date_df["ranking_bucket"] = [
            _ranking_bucket(int(position), eligible_count)
            if not np.isnan(position)
            else "tail"
            for position in eligible_rank
        ]
        date_df["primary_signal"] = date_df.apply(_primary_signal, axis=1)
        date_df["secondary_signal"] = date_df.apply(_secondary_signal, axis=1)
        date_df["is_top_pick"] = (
            (date_df["rank_position"] == 1)
            & date_df["is_feature_eligible"]
            & date_df["has_complete_features"]
        )
        ranked_frames.append(date_df)

    ranking_snapshot = pd.concat(ranked_frames, ignore_index=True)
    ranking_snapshot = _attach_history_deltas(ranking_snapshot, 7)
    ranking_snapshot = _attach_history_deltas(ranking_snapshot, 30)
    ranking_snapshot = ranking_snapshot[RANKING_SNAPSHOT_COLUMNS].copy()
    validate_ranking_snapshot(ranking_snapshot)
    return ranking_snapshot


def write_partitioned_parquet(
    df: pd.DataFrame,
    output_path: Path,
    file_prefix: str,
    partition_column: str = "reference_date",
) -> None:
    if output_path.exists():
        shutil.rmtree(output_path)

    output_path.mkdir(parents=True, exist_ok=True)

    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    df_to_save = df.copy()
    partition_dates = pd.to_datetime(df_to_save[partition_column])
    df_to_save["reference_year"] = partition_dates.dt.year.astype(str)
    df_to_save["reference_month"] = partition_dates.dt.month.astype(str).str.zfill(2)

    for (year, month), partition_df in df_to_save.groupby(
        ["reference_year", "reference_month"],
        sort=True,
    ):
        partition_dir = output_path / f"reference_year={year}" / f"reference_month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_dir / f"{file_prefix}_{run_date}.parquet"
        partition_df.drop(
            columns=["reference_year", "reference_month"]
        ).to_parquet(partition_file, index=False)


def _print_dataset_summary(df: pd.DataFrame, name: str) -> None:
    print(
        f"{name}: {len(df)} linhas, "
        f"{df['asset'].nunique()} ativos, "
        f"periodo {df['reference_date'].min()} -> {df['reference_date'].max()}"
    )


def main() -> int:
    print("Construindo Gold a partir da Silver...")

    try:
        prices_clean = load_silver_dataset(PRICES_CLEAN_PATH)
        asset_daily_status = load_silver_dataset(ASSET_DAILY_STATUS_PATH)

        asset_features = build_asset_features(prices_clean, asset_daily_status)
        ranking_snapshot = build_ranking_snapshot(asset_features, asset_daily_status)

        write_partitioned_parquet(
            asset_features,
            ASSET_FEATURES_OUTPUT,
            "asset_features",
        )
        write_partitioned_parquet(
            ranking_snapshot,
            RANKING_SNAPSHOT_OUTPUT,
            "ranking_snapshot",
        )

        _print_dataset_summary(asset_features, "asset_features")
        _print_dataset_summary(ranking_snapshot, "ranking_snapshot")
        print("Gold gerada com sucesso em data/gold.")
        return 0
    except (FileNotFoundError, GoldValidationError, ValueError) as exc:
        print(f"Erro na Gold: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

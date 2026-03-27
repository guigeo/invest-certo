from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from src.validators.silver_prices_validator import (
    SilverValidationError,
    validate_required_columns,
    validate_unique_key,
)


class GoldValidationError(SilverValidationError):
    """Raised when a Gold dataset fails validation."""


def _raise(message: str) -> None:
    raise GoldValidationError(message)


def _validate_finite(
    df: pd.DataFrame,
    columns: Iterable[str],
    dataset_name: str,
) -> None:
    for column in columns:
        non_null = df[column].dropna()
        if not np.isfinite(non_null).all():
            _raise(f"{dataset_name} contem valores infinitos ou invalidos em {column}.")


def validate_asset_features(df: pd.DataFrame) -> None:
    required_columns = [
        "reference_date",
        "asset",
        "ticker",
        "asset_type",
        "close",
        "adj_close",
        "data_points_252d",
        "feature_status",
        "momentum_bucket",
        "risk_bucket",
    ]
    validate_required_columns(df, required_columns, "Gold asset_features")
    validate_unique_key(df, ["asset", "reference_date"], "Gold asset_features")

    if df[["reference_date", "asset", "ticker", "asset_type", "close", "adj_close"]].isnull().any().any():
        _raise("Gold asset_features contem campos obrigatorios nulos.")

    if (df["close"] <= 0).any() or (df["adj_close"] <= 0).any():
        _raise("Gold asset_features requer precos de fechamento positivos.")

    if (df["data_points_252d"] < 0).any():
        _raise("data_points_252d deve ser maior ou igual a zero.")

    valid_feature_status = {
        "complete",
        "insufficient_history",
        "volume_missing",
        "calendar_gap_anomaly",
        "invalid_price",
    }
    unexpected_status = sorted(set(df["feature_status"]).difference(valid_feature_status))
    if unexpected_status:
        _raise(
            "feature_status contem valores inesperados: "
            f"{', '.join(unexpected_status)}"
        )

    valid_momentum_bucket = {"strong", "neutral", "weak"}
    if sorted(set(df["momentum_bucket"]).difference(valid_momentum_bucket)):
        _raise("momentum_bucket contem valores fora do dominio esperado.")

    valid_risk_bucket = {"low", "medium", "high"}
    if sorted(set(df["risk_bucket"]).difference(valid_risk_bucket)):
        _raise("risk_bucket contem valores fora do dominio esperado.")

    if (df["volatility_30d"].dropna() < 0).any():
        _raise("volatility_30d nao pode ser negativa.")

    drawdown = df["drawdown_252d"].dropna()
    if ((drawdown < -1) | (drawdown > 0)).any():
        _raise("drawdown_252d deve estar no intervalo [-1, 0].")

    _validate_finite(
        df,
        [
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
            "distance_to_ma20",
            "distance_to_ma90",
            "price_vs_52w_high",
            "price_vs_52w_low",
        ],
        "Gold asset_features",
    )


def validate_ranking_snapshot(df: pd.DataFrame) -> None:
    required_columns = [
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
        "primary_signal",
        "secondary_signal",
        "is_top_pick",
    ]
    validate_required_columns(df, required_columns, "Gold ranking_snapshot")
    validate_unique_key(df, ["reference_date", "asset"], "Gold ranking_snapshot")

    if df[required_columns].isnull().any().any():
        _raise("Gold ranking_snapshot contem campos obrigatorios nulos.")

    if (df["rank_position"] <= 0).any():
        _raise("rank_position deve ser inteiro positivo.")

    valid_status = {
        "eligible",
        "insufficient_history",
        "volume_missing",
        "calendar_gap_anomaly",
        "invalid_price",
    }
    unexpected_status = sorted(set(df["eligibility_status"]).difference(valid_status))
    if unexpected_status:
        _raise(
            "eligibility_status contem valores inesperados: "
            f"{', '.join(unexpected_status)}"
        )

    valid_buckets = {"top_3", "top_5", "middle", "tail"}
    unexpected_buckets = sorted(set(df["ranking_bucket"]).difference(valid_buckets))
    if unexpected_buckets:
        _raise(
            "ranking_bucket contem valores inesperados: "
            f"{', '.join(unexpected_buckets)}"
        )

    _validate_finite(
        df,
        ["score", "rank_delta_7d", "rank_delta_30d", "score_delta_7d", "score_delta_30d"],
        "Gold ranking_snapshot",
    )

    for reference_date, date_df in df.groupby("reference_date"):
        ordered = date_df.sort_values("rank_position")
        expected_positions = list(range(1, len(ordered) + 1))
        actual_positions = ordered["rank_position"].tolist()
        if actual_positions != expected_positions:
            _raise(
                "rank_position contem lacunas ou ordem invalida em "
                f"{reference_date.date()}."
            )

        eligible = ordered[ordered["eligibility_status"] == "eligible"]
        if not eligible["score"].is_monotonic_decreasing:
            _raise(
                "score deve estar ordenado de forma decrescente entre ativos elegiveis "
                f"em {reference_date.date()}."
            )

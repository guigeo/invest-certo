import pandas as pd
import pytest

from src.validators.bronze_prices_validator import (
    BronzePricesValidationError,
    validate_bronze_prices,
)


def build_valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [10.0, 10.5],
            "high": [10.3, 10.8],
            "low": [9.9, 10.2],
            "close": [10.1, 10.7],
            "adj_close": [10.1, 10.7],
            "volume": [1000, 1200],
            "asset": ["BBAS3", "BBAS3"],
            "ticker": ["BBAS3.SA", "BBAS3.SA"],
        }
    )


def test_validate_bronze_prices_accepts_float_precision_noise() -> None:
    df = build_valid_frame()
    df.loc[0, "close"] = 10.3004
    df.loc[0, "high"] = 10.3
    df.loc[1, "close"] = 10.2
    df.loc[1, "low"] = 10.2004

    validate_bronze_prices(df)


def test_validate_bronze_prices_rejects_price_gap_above_tolerance() -> None:
    df = build_valid_frame()
    df.loc[0, "close"] = 10.302
    df.loc[0, "high"] = 10.3

    with pytest.raises(BronzePricesValidationError, match="tolerancia de 0.001"):
        validate_bronze_prices(df)


def test_validate_bronze_prices_accepts_open_within_tolerance() -> None:
    df = build_valid_frame()
    df.loc[0, "open"] = 10.3005
    df.loc[0, "high"] = 10.3

    validate_bronze_prices(df)


def test_validate_bronze_prices_rejects_open_outside_range() -> None:
    df = build_valid_frame()
    df.loc[0, "open"] = 10.304
    df.loc[0, "high"] = 10.3

    with pytest.raises(BronzePricesValidationError, match="Amostra"):
        validate_bronze_prices(df)

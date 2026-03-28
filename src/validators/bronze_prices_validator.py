from typing import Iterable

import pandas as pd
from pandas.api.types import is_numeric_dtype

from src.validators.price_rules import PRICE_TOLERANCE


REQUIRED_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "asset",
    "ticker",
)

CRITICAL_COLUMNS = REQUIRED_COLUMNS
PRICE_COLUMNS = ("open", "high", "low", "close", "adj_close")


class BronzePricesValidationError(ValueError):
    """Raised when Bronze prices validation fails."""


def validate_bronze_prices(df: pd.DataFrame) -> None:
    if df.empty:
        raise BronzePricesValidationError(
            "O dataset Bronze prices esta vazio. Nada deve ser salvo."
        )

    _validate_schema(df)
    _validate_types(df)
    _validate_nulls(df, CRITICAL_COLUMNS)
    _validate_duplicates(df)
    _validate_non_negative_prices(df)
    _validate_volume(df)
    _validate_price_consistency(df)


def _validate_schema(df: pd.DataFrame) -> None:
    missing_columns = sorted(set(REQUIRED_COLUMNS).difference(df.columns))
    if missing_columns:
        raise BronzePricesValidationError(
            "Schema invalido para Bronze prices. "
            f"Colunas ausentes: {', '.join(missing_columns)}"
        )


def _validate_types(df: pd.DataFrame) -> None:
    parsed_dates = pd.to_datetime(df["date"], errors="coerce")
    invalid_dates = df[parsed_dates.isna()]
    if not invalid_dates.empty:
        sample = invalid_dates[["asset", "date"]].head(5).to_dict("records")
        raise BronzePricesValidationError(
            "Valores invalidos encontrados na coluna date. "
            f"Amostra: {sample}"
        )

    numeric_columns = ("open", "high", "low", "close", "adj_close", "volume")
    invalid_numeric = [
        column for column in numeric_columns
        if not is_numeric_dtype(df[column])
    ]
    if invalid_numeric:
        raise BronzePricesValidationError(
            "Colunas numericas com tipo invalido em Bronze prices. "
            f"Colunas: {', '.join(invalid_numeric)}"
        )


def _validate_nulls(df: pd.DataFrame, columns: Iterable[str]) -> None:
    null_counts = df[list(columns)].isnull().sum()
    invalid_columns = {
        column: int(count)
        for column, count in null_counts.items()
        if count > 0
    }
    if invalid_columns:
        raise BronzePricesValidationError(
            "Valores nulos encontrados em colunas criticas: "
            f"{invalid_columns}"
        )


def _validate_duplicates(df: pd.DataFrame) -> None:
    duplicates = df[df.duplicated(subset=["asset", "date"], keep=False)]
    if not duplicates.empty:
        sample = duplicates[["asset", "date"]].head(5).to_dict("records")
        raise BronzePricesValidationError(
            "Duplicidade encontrada para a chave unica asset + date. "
            f"Amostra: {sample}"
        )


def _validate_non_negative_prices(df: pd.DataFrame) -> None:
    invalid_rows = df[(df[list(PRICE_COLUMNS)] < 0).any(axis=1)]
    if not invalid_rows.empty:
        sample = invalid_rows[["asset", "date", "open", "high", "low", "close", "adj_close"]]
        raise BronzePricesValidationError(
            "Precos negativos encontrados no Bronze prices. "
            f"Amostra: {sample.head(5).to_dict('records')}"
        )


def _validate_volume(df: pd.DataFrame) -> None:
    invalid_rows = df[df["volume"] < 0]
    if not invalid_rows.empty:
        sample = invalid_rows[["asset", "date", "volume"]].head(5).to_dict("records")
        raise BronzePricesValidationError(
            "Volumes negativos encontrados no Bronze prices. "
            f"Amostra: {sample}"
        )


def _validate_price_consistency(df: pd.DataFrame) -> None:
    invalid_rows = df[
        ~(
            (df["low"] <= df["close"] + PRICE_TOLERANCE)
            & (df["close"] <= df["high"] + PRICE_TOLERANCE)
            & (df["low"] <= df["open"] + PRICE_TOLERANCE)
            & (df["open"] <= df["high"] + PRICE_TOLERANCE)
        )
    ]
    if not invalid_rows.empty:
        sample = invalid_rows[["asset", "date", "low", "open", "close", "high"]]
        raise BronzePricesValidationError(
            "Inconsistencia de preco encontrada. "
            "A regra esperada e low <= close <= high, "
            "com open dentro do intervalo [low, high], "
            f"usando tolerancia de {PRICE_TOLERANCE}. "
            f"Amostra: {sample.head(5).to_dict('records')}"
        )

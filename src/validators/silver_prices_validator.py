from __future__ import annotations

from typing import Iterable

import pandas as pd


class SilverValidationError(ValueError):
    """Raised when a Silver dataset fails validation."""


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str], dataset_name: str) -> None:
    missing_columns = sorted(set(required_columns).difference(df.columns))
    if missing_columns:
        raise SilverValidationError(
            f"Schema invalido para {dataset_name}. "
            f"Colunas ausentes: {', '.join(missing_columns)}"
        )


def validate_unique_key(df: pd.DataFrame, key_columns: list[str], dataset_name: str) -> None:
    duplicates = df[df.duplicated(subset=key_columns, keep=False)]
    if not duplicates.empty:
        sample = duplicates[key_columns].head(5).to_dict("records")
        raise SilverValidationError(
            f"Duplicidade encontrada em {dataset_name} para a chave "
            f"{' + '.join(key_columns)}. Amostra: {sample}"
        )


def validate_non_negative(df: pd.DataFrame, columns: Iterable[str], dataset_name: str) -> None:
    invalid_rows = df[(df[list(columns)] < 0).any(axis=1)]
    if not invalid_rows.empty:
        sample = invalid_rows.head(5).to_dict("records")
        raise SilverValidationError(
            f"Valores negativos encontrados em {dataset_name}. Amostra: {sample}"
        )


from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys
import duckdb
import pandas as pd

from src.collect.reader import read_assets
from src.validators.silver_prices_validator import (
    SilverValidationError,
    validate_non_negative,
    validate_required_columns,
    validate_unique_key,
)


ASSETS_FILE = Path("config/assets.txt")
BRONZE_PATH = Path("data/bronze/prices")
SILVER_BASE_PATH = Path("data/silver")
PRICES_CLEAN_OUTPUT = SILVER_BASE_PATH / "prices_clean"
ASSET_DAILY_STATUS_OUTPUT = SILVER_BASE_PATH / "asset_daily_status"
MAX_EXPECTED_GAP_DAYS = 5
PRICE_TOLERANCE = 1e-3

PRICES_CLEAN_COLUMNS = [
    "date",
    "asset",
    "ticker",
    "asset_type",
    "source",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "is_volume_missing",
]

ASSET_DAILY_STATUS_COLUMNS = [
    "date",
    "asset",
    "ticker",
    "asset_type",
    "is_price_valid",
    "is_volume_missing",
    "is_zero_volume",
    "prev_date",
    "days_since_prev_trade",
    "has_calendar_gap_anomaly",
    "history_length_days",
    "has_min_history_30d",
    "has_min_history_90d",
    "has_min_history_252d",
    "is_feature_eligible",
    "eligibility_status",
]


def load_assets_catalog(file_path: Path = ASSETS_FILE) -> pd.DataFrame:
    assets = read_assets(file_path)
    assets_df = pd.DataFrame(assets).rename(columns={"type": "asset_type"})
    return assets_df[["asset", "asset_type", "source", "ticker"]]


def build_prices_clean(
    bronze_path: Path,
    assets_df: pd.DataFrame,
) -> pd.DataFrame:
    if not bronze_path.exists():
        raise FileNotFoundError(f"Diretorio Bronze nao encontrado: {bronze_path}")

    parquet_files = list(bronze_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum parquet encontrado em: {bronze_path}")

    con = duckdb.connect(database=":memory:")
    try:
        con.register("assets_catalog", assets_df)
        bronze_glob = str(bronze_path / "**/*.parquet").replace("'", "''")

        duplicate_check = con.execute(
            f"""
            WITH bronze_enriched AS (
                SELECT
                    CAST(b.date AS DATE) AS date,
                    CAST(b.open AS DOUBLE) AS open,
                    CAST(b.high AS DOUBLE) AS high,
                    CAST(b.low AS DOUBLE) AS low,
                    CAST(b.close AS DOUBLE) AS close,
                    CAST(b.adj_close AS DOUBLE) AS adj_close,
                    CAST(b.volume AS BIGINT) AS volume,
                    TRIM(b.asset) AS asset,
                    TRIM(b.ticker) AS ticker,
                    assets_catalog.asset_type AS asset_type,
                    assets_catalog.source AS source
                FROM read_parquet('{bronze_glob}') AS b
                LEFT JOIN assets_catalog
                  ON TRIM(b.asset) = assets_catalog.asset
            ),
            duplicate_rows AS (
                SELECT
                    asset,
                    date,
                    COUNT(*) AS total_rows,
                    COUNT(
                        DISTINCT md5(
                            COALESCE(ticker, '') || '|' ||
                            COALESCE(asset_type, '') || '|' ||
                            COALESCE(source, '') || '|' ||
                            COALESCE(CAST(open AS VARCHAR), '') || '|' ||
                            COALESCE(CAST(high AS VARCHAR), '') || '|' ||
                            COALESCE(CAST(low AS VARCHAR), '') || '|' ||
                            COALESCE(CAST(close AS VARCHAR), '') || '|' ||
                            COALESCE(CAST(adj_close AS VARCHAR), '') || '|' ||
                            COALESCE(CAST(volume AS VARCHAR), '')
                        )
                    ) AS distinct_payloads
                FROM bronze_enriched
                GROUP BY asset, date
                HAVING COUNT(*) > 1
            )
            SELECT *
            FROM duplicate_rows
            WHERE distinct_payloads > 1
            """
        ).fetchdf()

        if not duplicate_check.empty:
            sample = duplicate_check.head(5).to_dict("records")
            raise SilverValidationError(
                "Duplicidade conflitante encontrada na Bronze para asset + date. "
                f"Amostra: {sample}"
            )

        prices_clean = con.execute(
            f"""
            WITH bronze_enriched AS (
                SELECT
                    CAST(b.date AS DATE) AS date,
                    CAST(b.open AS DOUBLE) AS open,
                    CAST(b.high AS DOUBLE) AS high,
                    CAST(b.low AS DOUBLE) AS low,
                    CAST(b.close AS DOUBLE) AS close,
                    CAST(b.adj_close AS DOUBLE) AS adj_close,
                    CAST(b.volume AS BIGINT) AS volume,
                    TRIM(b.asset) AS asset,
                    TRIM(b.ticker) AS ticker,
                    assets_catalog.asset_type AS asset_type,
                    assets_catalog.source AS source
                FROM read_parquet('{bronze_glob}') AS b
                LEFT JOIN assets_catalog
                  ON TRIM(b.asset) = assets_catalog.asset
            )
            SELECT DISTINCT
                date,
                asset,
                ticker,
                asset_type,
                source,
                open,
                high,
                low,
                close,
                adj_close,
                COALESCE(volume, 0) AS volume,
                volume IS NULL AS is_volume_missing
            FROM bronze_enriched
            WHERE NOT (
                open = 0
                AND high = 0
                AND low = 0
                AND close > 0
                AND COALESCE(volume, 0) = 0
            )
            ORDER BY asset, date
            """
        ).fetchdf()
    finally:
        con.close()

    validate_prices_clean(prices_clean)
    return prices_clean[PRICES_CLEAN_COLUMNS]


def validate_prices_clean(df: pd.DataFrame) -> None:
    validate_required_columns(df, PRICES_CLEAN_COLUMNS, "Silver prices_clean")
    validate_unique_key(df, ["asset", "date"], "Silver prices_clean")

    if df[["asset", "ticker", "asset_type", "source"]].isnull().any().any():
        raise SilverValidationError(
            "Ativo fora do cadastro ou metadata ausente em Silver prices_clean."
        )

    if df["date"].isnull().any():
        raise SilverValidationError("Valores invalidos encontrados em date.")

    if df[["open", "high", "low", "close", "adj_close", "volume"]].isnull().any().any():
        raise SilverValidationError("Valores nulos encontrados em colunas numericas.")

    validate_non_negative(
        df,
        ["open", "high", "low", "close", "adj_close", "volume"],
        "Silver prices_clean",
    )

    invalid_rows = df[
        ~(
            (df["low"] <= df["close"] + PRICE_TOLERANCE)
            & (df["close"] <= df["high"] + PRICE_TOLERANCE)
            & (df["low"] <= df["open"] + PRICE_TOLERANCE)
            & (df["open"] <= df["high"] + PRICE_TOLERANCE)
        )
    ]
    if not invalid_rows.empty:
        sample = invalid_rows.head(5).to_dict("records")
        raise SilverValidationError(
            "Inconsistencia de preco encontrada em Silver prices_clean. "
            f"Amostra: {sample}"
        )


def is_provider_anomaly_row(df: pd.DataFrame) -> pd.Series:
    return (
        (df["open"] == 0)
        & (df["high"] == 0)
        & (df["low"] == 0)
        & (df["close"] > 0)
        & (df["volume"] == 0)
    )


def build_asset_daily_status(prices_clean: pd.DataFrame) -> pd.DataFrame:
    status_df = prices_clean.sort_values(["asset", "date"]).copy()

    status_df["prev_date"] = status_df.groupby("asset")["date"].shift(1)
    status_df["days_since_prev_trade"] = (
        status_df["date"] - status_df["prev_date"]
    ).dt.days
    status_df["has_calendar_gap_anomaly"] = (
        status_df["days_since_prev_trade"].fillna(0) > MAX_EXPECTED_GAP_DAYS
    )
    status_df["history_length_days"] = status_df.groupby("asset").cumcount() + 1
    status_df["has_min_history_30d"] = status_df["history_length_days"] >= 30
    status_df["has_min_history_90d"] = status_df["history_length_days"] >= 90
    status_df["has_min_history_252d"] = status_df["history_length_days"] >= 252
    status_df["is_zero_volume"] = status_df["volume"] == 0
    status_df["is_price_valid"] = (
        (status_df[["open", "high", "low", "close", "adj_close"]] >= 0).all(axis=1)
        & (status_df["low"] <= status_df["close"] + PRICE_TOLERANCE)
        & (status_df["close"] <= status_df["high"] + PRICE_TOLERANCE)
        & (status_df["low"] <= status_df["open"] + PRICE_TOLERANCE)
        & (status_df["open"] <= status_df["high"] + PRICE_TOLERANCE)
    )

    status_df["eligibility_status"] = "eligible"
    status_df.loc[~status_df["is_price_valid"], "eligibility_status"] = "invalid_price"
    status_df.loc[
        status_df["is_price_valid"] & status_df["has_calendar_gap_anomaly"],
        "eligibility_status",
    ] = "calendar_gap_anomaly"
    status_df.loc[
        status_df["is_price_valid"]
        & ~status_df["has_calendar_gap_anomaly"]
        & ~status_df["has_min_history_252d"],
        "eligibility_status",
    ] = "insufficient_history"
    status_df.loc[
        status_df["is_price_valid"]
        & ~status_df["has_calendar_gap_anomaly"]
        & status_df["has_min_history_252d"]
        & status_df["is_volume_missing"],
        "eligibility_status",
    ] = "volume_missing"

    status_df["is_feature_eligible"] = status_df["eligibility_status"] == "eligible"

    result = status_df[ASSET_DAILY_STATUS_COLUMNS].copy()
    validate_asset_daily_status(result)
    return result


def validate_asset_daily_status(df: pd.DataFrame) -> None:
    validate_required_columns(df, ASSET_DAILY_STATUS_COLUMNS, "Silver asset_daily_status")
    validate_unique_key(df, ["asset", "date"], "Silver asset_daily_status")

    if df["date"].isnull().any():
        raise SilverValidationError("Valores invalidos encontrados em date de asset_daily_status.")

    if (df["history_length_days"] <= 0).any():
        raise SilverValidationError("history_length_days deve ser maior que zero.")

    valid_status = {
        "eligible",
        "insufficient_history",
        "volume_missing",
        "calendar_gap_anomaly",
        "invalid_price",
    }
    unexpected = sorted(set(df["eligibility_status"]).difference(valid_status))
    if unexpected:
        raise SilverValidationError(
            "eligibility_status contem valores inesperados: "
            f"{', '.join(unexpected)}"
        )


def write_partitioned_parquet(
    df: pd.DataFrame,
    output_path: Path,
    file_prefix: str,
    partition_column: str = "date",
) -> None:
    if output_path.exists():
        shutil.rmtree(output_path)

    output_path.mkdir(parents=True, exist_ok=True)

    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    df_to_save = df.copy()
    partition_dates = pd.to_datetime(df_to_save[partition_column])
    df_to_save["year"] = partition_dates.dt.year.astype(str)
    df_to_save["month"] = partition_dates.dt.month.astype(str).str.zfill(2)

    for (year, month), partition_df in df_to_save.groupby(["year", "month"], sort=True):
        partition_dir = output_path / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_dir / f"{file_prefix}_{run_date}.parquet"
        partition_df.drop(columns=["year", "month"]).to_parquet(partition_file, index=False)


def _print_dataset_summary(df: pd.DataFrame, name: str) -> None:
    print(
        f"{name}: {len(df)} linhas, "
        f"{df['asset'].nunique()} ativos, "
        f"periodo {df['date'].min()} -> {df['date'].max()}"
    )


def main() -> int:
    print("Transformando Bronze em Silver...")

    try:
        assets_df = load_assets_catalog(ASSETS_FILE)
        prices_clean = build_prices_clean(BRONZE_PATH, assets_df)
        asset_daily_status = build_asset_daily_status(prices_clean)

        write_partitioned_parquet(prices_clean, PRICES_CLEAN_OUTPUT, "prices_clean")
        write_partitioned_parquet(
            asset_daily_status,
            ASSET_DAILY_STATUS_OUTPUT,
            "asset_daily_status",
        )

        _print_dataset_summary(prices_clean, "prices_clean")
        _print_dataset_summary(asset_daily_status, "asset_daily_status")
        print("Silver gerada com sucesso em data/silver.")
        return 0
    except (FileNotFoundError, SilverValidationError, ValueError) as exc:
        print(f"Erro na Silver: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

from pathlib import Path

import pandas as pd
import pytest

from pipelines.silver.transform_prices import (
    build_asset_daily_status,
    build_prices_clean,
    is_provider_anomaly_row,
    load_assets_catalog,
)


def create_bronze_dataset(base_path: Path) -> None:
    partition = base_path / "asset=BBAS3" / "year=2024" / "month=01"
    partition.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-03", "2024-01-10"]
            ),
            "open": [10.0, 10.5, 10.5, 11.0],
            "high": [10.3, 10.8, 10.8, 11.2],
            "low": [9.9, 10.2, 10.2, 10.7],
            "close": [10.1, 10.7, 10.7, 11.1],
            "adj_close": [10.1, 10.7, 10.7, 11.1],
            "volume": [1000, 0, 0, 1500],
            "asset": ["BBAS3", "BBAS3", "BBAS3", "BBAS3"],
            "ticker": ["BBAS3.SA", "BBAS3.SA", "BBAS3.SA", "BBAS3.SA"],
        }
    )
    df.to_parquet(partition / "prices_20240110T000000Z.parquet", index=False)


def create_assets_file(file_path: Path) -> None:
    file_path.write_text(
        "BBAS3|stock|yahoo|BBAS3.SA\n",
        encoding="utf-8",
    )


def test_build_prices_clean_deduplicates_and_enriches(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    assets_file = tmp_path / "config" / "assets.txt"
    assets_file.parent.mkdir(parents=True, exist_ok=True)

    create_bronze_dataset(bronze_path)
    create_assets_file(assets_file)

    assets_df = load_assets_catalog(assets_file)
    result = build_prices_clean(bronze_path, assets_df)

    assert list(result.columns) == [
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
    assert len(result) == 3
    assert result["asset_type"].tolist() == ["stock", "stock", "stock"]
    assert result["date"].dtype.name.startswith("datetime64")


def test_build_asset_daily_status_computes_gap_and_history_flags(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    assets_file = tmp_path / "config" / "assets.txt"
    assets_file.parent.mkdir(parents=True, exist_ok=True)

    create_bronze_dataset(bronze_path)
    create_assets_file(assets_file)

    assets_df = load_assets_catalog(assets_file)
    prices_clean = build_prices_clean(bronze_path, assets_df)
    status = build_asset_daily_status(prices_clean)

    jan_10 = status.loc[status["date"] == pd.Timestamp("2024-01-10")].iloc[0]

    assert jan_10["prev_date"] == pd.Timestamp("2024-01-03")
    assert jan_10["days_since_prev_trade"] == 7
    assert bool(jan_10["has_calendar_gap_anomaly"]) is True
    assert bool(jan_10["has_min_history_30d"]) is False
    assert jan_10["eligibility_status"] == "calendar_gap_anomaly"


def test_build_prices_clean_rejects_conflicting_duplicates(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    assets_file = tmp_path / "config" / "assets.txt"
    assets_file.parent.mkdir(parents=True, exist_ok=True)

    partition = bronze_path / "asset=BBAS3" / "year=2024" / "month=01"
    partition.mkdir(parents=True, exist_ok=True)
    create_assets_file(assets_file)

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "open": [10.0, 99.0],
            "high": [10.3, 99.3],
            "low": [9.9, 98.9],
            "close": [10.1, 99.1],
            "adj_close": [10.1, 99.1],
            "volume": [1000, 1000],
            "asset": ["BBAS3", "BBAS3"],
            "ticker": ["BBAS3.SA", "BBAS3.SA"],
        }
    )
    df.to_parquet(partition / "prices_20240102T000000Z.parquet", index=False)

    assets_df = load_assets_catalog(assets_file)

    with pytest.raises(ValueError, match="Duplicidade conflitante"):
        build_prices_clean(bronze_path, assets_df)


def test_provider_anomaly_rows_are_filtered_from_prices_clean(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    assets_file = tmp_path / "config" / "assets.txt"
    assets_file.parent.mkdir(parents=True, exist_ok=True)
    create_assets_file(assets_file)

    partition = bronze_path / "asset=BBAS3" / "year=2024" / "month=01"
    partition.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [10.0, 0.0],
            "high": [10.3, 0.0],
            "low": [9.9, 0.0],
            "close": [10.1, 11.5],
            "adj_close": [10.1, 11.5],
            "volume": [1000, 0],
            "asset": ["BBAS3", "BBAS3"],
            "ticker": ["BBAS3.SA", "BBAS3.SA"],
        }
    )
    df.to_parquet(partition / "prices_20240103T000000Z.parquet", index=False)

    assert is_provider_anomaly_row(df).tolist() == [False, True]

    assets_df = load_assets_catalog(assets_file)
    result = build_prices_clean(bronze_path, assets_df)

    assert len(result) == 1
    assert result["date"].iloc[0] == pd.Timestamp("2024-01-02")

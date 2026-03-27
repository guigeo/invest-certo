from pathlib import Path

import pandas as pd
import pytest

from pipelines.bronze.query_prices import execute_query, read_sql_file, render_result


def create_bronze_dataset(base_path: Path) -> None:
    partition = base_path / "asset=BBAS3" / "year=2024" / "month=01"
    partition.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
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
    df.to_parquet(partition / "prices_20240103T000000Z.parquet", index=False)


def test_execute_query_returns_data(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    create_bronze_dataset(bronze_path)

    result = execute_query(
        "SELECT asset, COUNT(*) AS total_rows FROM bronze_prices GROUP BY asset",
        base_path=bronze_path,
    )

    assert result.to_dict("records") == [{"asset": "BBAS3", "total_rows": 2}]


def test_execute_query_raises_when_bronze_is_missing(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"

    with pytest.raises(FileNotFoundError, match="Diretorio Bronze nao encontrado"):
        execute_query("SELECT 1", base_path=bronze_path)


def test_execute_query_raises_for_invalid_sql(tmp_path: Path) -> None:
    bronze_path = tmp_path / "data" / "bronze" / "prices"
    create_bronze_dataset(bronze_path)

    with pytest.raises(ValueError, match="Erro ao executar SQL"):
        execute_query("SELECT FROM bronze_prices", base_path=bronze_path)


def test_read_sql_file_rejects_empty_file(tmp_path: Path) -> None:
    sql_file = tmp_path / "empty.sql"
    sql_file.write_text("   \n", encoding="utf-8")

    with pytest.raises(ValueError, match="Arquivo SQL vazio"):
        read_sql_file(sql_file)


def test_render_result_reports_empty_dataframe() -> None:
    result = render_result(pd.DataFrame(columns=["asset", "ticker"]))

    assert "nao retornou linhas" in result

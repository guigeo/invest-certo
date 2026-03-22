from pathlib import Path
import pandas as pd


BASE_PATH = Path("data/bronze/prices")
EXPECTED_COLUMNS = {
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "asset",
    "ticker",
}


def load_data() -> pd.DataFrame:
    dfs = []

    for file_path in BASE_PATH.rglob("*.parquet"):
        df = pd.read_parquet(file_path)
        dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum dado encontrado.")

    return pd.concat(dfs, ignore_index=True)


def validate_schema(df: pd.DataFrame):
    missing_columns = EXPECTED_COLUMNS.difference(df.columns)
    assert not missing_columns, f"Colunas ausentes: {sorted(missing_columns)}"


def validate_shape(df: pd.DataFrame):
    assert df.shape[0] > 0, "A Bronze precisa ter ao menos uma linha."
    assert df.shape[1] == len(EXPECTED_COLUMNS), "Quantidade de colunas inesperada."


def validate_dates(df: pd.DataFrame):
    assert df["date"].notna().all(), "Existem datas nulas na Bronze."
    assert df["date"].min() <= df["date"].max(), "Intervalo de datas inválido."


def validate_nulls(df: pd.DataFrame):
    required_non_null = ["date", "close", "asset", "ticker"]
    nulls = df[required_non_null].isnull().sum()
    assert (nulls == 0).all(), f"Campos obrigatórios com nulos: {nulls.to_dict()}"


def validate_duplicates(df: pd.DataFrame):
    duplicates = df.duplicated(subset=["asset", "ticker", "date"]).sum()
    assert duplicates == 0, f"Duplicados por ativo/ticker/data: {duplicates}"


def validate_assets_distribution(df: pd.DataFrame):
    asset_counts = df.groupby("asset").size()
    assert (asset_counts > 0).all(), "Existe ativo sem registros válidos."


def validate_last_price(df: pd.DataFrame):
    df_last = (
        df.sort_values("date")
        .groupby("asset")
        .tail(1)
        .sort_values("asset")
    )

    assert not df_last.empty, "Não foi possível calcular o último preço por ativo."
    assert (df_last["close"] > 0).all(), "Existem últimos preços não positivos."


def test_bronze_data_contract():
    df = load_data()

    validate_shape(df)
    validate_schema(df)
    validate_dates(df)
    validate_nulls(df)
    validate_duplicates(df)
    validate_assets_distribution(df)
    validate_last_price(df)

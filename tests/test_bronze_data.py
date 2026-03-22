from pathlib import Path
import pandas as pd


BASE_PATH = Path("data/bronze/prices")


def load_data() -> pd.DataFrame:
    dfs = []

    for asset_dir in BASE_PATH.glob("asset=*"):
        file_path = asset_dir / "prices.parquet"

        if not file_path.exists():
            print(f"Arquivo não encontrado: {file_path}")
            continue

        df = pd.read_parquet(file_path)
        dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum dado encontrado.")

    return pd.concat(dfs, ignore_index=True)


def validate_schema(df: pd.DataFrame):
    print("\n📌 SCHEMA")
    print(df.dtypes)
    print(f"Colunas: {list(df.columns)}")


def validate_shape(df: pd.DataFrame):
    print("\n📊 SHAPE")
    print(f"Linhas: {df.shape[0]}")
    print(f"Colunas: {df.shape[1]}")


def validate_dates(df: pd.DataFrame):
    print("\n📅 INTERVALO DE DATAS")
    print(f"Min: {df['date'].min()}")
    print(f"Max: {df['date'].max()}")


def validate_nulls(df: pd.DataFrame):
    print("\n⚠️ VALORES NULOS")
    print(df.isnull().sum())


def validate_duplicates(df: pd.DataFrame):
    print("\n🔁 DUPLICADOS")
    print(f"Duplicados: {df.duplicated().sum()}")


def validate_assets_distribution(df: pd.DataFrame):
    print("\n📊 REGISTROS POR ATIVO")
    print(df.groupby("asset").size().sort_values(ascending=False))


def validate_last_price(df: pd.DataFrame):
    print("\n💰 ÚLTIMO PREÇO POR ATIVO")

    df_last = (
        df.sort_values("date")
        .groupby("asset")
        .tail(1)
        .sort_values("asset")
    )

    print(df_last[["asset", "date", "close"]])


def main():
    print("🚀 Iniciando validação Bronze...")

    df = load_data()

    validate_shape(df)
    validate_schema(df)
    validate_dates(df)
    validate_nulls(df)
    validate_duplicates(df)
    validate_assets_distribution(df)
    validate_last_price(df)

    print("\n✅ Validação concluída.")


if __name__ == "__main__":
    main()
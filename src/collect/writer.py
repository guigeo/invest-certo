from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import pandas as pd


def get_latest_stored_date(asset: str, base_path: str) -> Optional[pd.Timestamp]:
    base_dir = Path(base_path) / f"asset={asset}"
    if not base_dir.exists():
        return None

    latest_date = None

    for parquet_file in base_dir.rglob("*.parquet"):
        df = pd.read_parquet(parquet_file, columns=["date"])
        if df.empty:
            continue

        current_max = pd.to_datetime(df["date"], utc=True).max()
        if pd.isna(current_max):
            continue

        current_max = current_max.tz_localize(None) if getattr(current_max, "tzinfo", None) else current_max

        if latest_date is None or current_max > latest_date:
            latest_date = current_max

    return latest_date


def save_price_history(df: pd.DataFrame, base_path: str) -> Path:
    # 🔹 1. validação básica
    if df.empty:
        raise ValueError("DataFrame vazio. Nada para salvar.")

    # 🔹 2. transforma caminho em objeto Path
    base_path = Path(base_path)

    # 🔹 3. identifica o ativo
    asset = df["asset"].iloc[0]

    collected_at = datetime.now(timezone.utc)
    ingestion_timestamp = collected_at.strftime("%Y%m%dT%H%M%SZ")

    df_to_save = df.copy()
    df_to_save["date"] = pd.to_datetime(df_to_save["date"], utc=True).dt.tz_localize(None)
    df_to_save["year"] = df_to_save["date"].dt.year.astype(str)
    df_to_save["month"] = df_to_save["date"].dt.month.astype(str).str.zfill(2)

    output_file = None

    # 🔹 4. cria estrutura particionada por ativo, ano e mês
    for (year, month), partition_df in df_to_save.groupby(["year", "month"], sort=True):
        output_dir = base_path / f"asset={asset}" / f"year={year}" / f"month={month}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"prices_{ingestion_timestamp}.parquet"
        partition_df.drop(columns=["year", "month"]).to_parquet(output_file, index=False)

    # 🔹 5. retorna o último caminho salvo
    return output_file

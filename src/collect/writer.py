from datetime import datetime, timezone
from pathlib import Path
import pandas as pd


def save_price_history(df: pd.DataFrame, base_path: str) -> Path:
    # 🔹 1. validação básica
    if df.empty:
        raise ValueError("DataFrame vazio. Nada para salvar.")

    # 🔹 2. transforma caminho em objeto Path
    base_path = Path(base_path)

    # 🔹 3. identifica o ativo
    asset = df["asset"].iloc[0]

    collected_at = datetime.now(timezone.utc)
    ingestion_date = collected_at.strftime("%Y-%m-%d")
    ingestion_timestamp = collected_at.strftime("%Y%m%dT%H%M%SZ")

    # 🔹 4. cria estrutura particionada
    output_dir = base_path / f"asset={asset}" / f"ingestion_date={ingestion_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🔹 5. define nome do arquivo
    output_file = output_dir / f"prices_{ingestion_timestamp}.parquet"

    # 🔹 6. salva parquet
    df.to_parquet(output_file, index=False)

    # 🔹 7. retorna caminho salvo
    return output_file

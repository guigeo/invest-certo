# Pipeline de ingestão de dados

from datetime import timedelta
from pathlib import Path
import sys
from src.collect.reader import read_assets
from src.collect.fetcher import fetch_price_history
from src.collect.writer import get_latest_stored_date, save_price_history
from src.validators.bronze_prices_validator import validate_bronze_prices



ASSETS_FILE = Path("config/assets.txt")
OUTPUT_PATH = "data/bronze/prices"


def main() -> int:
    print("🚀 Iniciando coleta Bronze...")

    assets = read_assets(ASSETS_FILE)
    print(f"Total de ativos: {len(assets)}")

    success = 0
    empty = 0

    for asset_info in assets:
        asset = asset_info["asset"]
        ticker = asset_info["ticker"]

        print(f"\n📊 Processando: {asset} ({ticker})")

        try:
            last_stored_date = get_latest_stored_date(asset, OUTPUT_PATH)
            start_date = None

            if last_stored_date is not None:
                start_date = (last_stored_date + timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"Coleta incremental a partir de: {start_date}")
            else:
                print("Coleta inicial completa desde 2015-01-01")

            df = fetch_price_history(asset_info, start_date=start_date)

            if df.empty:
                print(f"⚠️ Sem novos dados para {asset}")
                empty += 1
                continue

            validate_bronze_prices(df)
            path = save_price_history(df, OUTPUT_PATH)

            print(f"✅ Salvo em: {path}")
            success += 1

        except Exception as e:
            print(f"❌ Erro em {asset}: {e}")
            return 1

    print("\n📦 Resumo:")
    print(f"✔ Sucesso: {success}")
    print(f"⚠️ Sem dados: {empty}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

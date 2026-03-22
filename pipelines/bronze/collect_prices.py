# Pipeline de ingestão de dados

from datetime import timedelta
from pathlib import Path
import sys
from src.collect.reader import read_assets
from src.collect.fetcher import fetch_price_history
from src.collect.writer import get_latest_stored_date, save_price_history



ASSETS_FILE = Path("config/assets.txt")
OUTPUT_PATH = "data/bronze/prices"


def main() -> int:
    print("🚀 Iniciando coleta Bronze...")

    assets = read_assets(ASSETS_FILE)
    print(f"Total de ativos: {len(assets)}")

    success = 0
    failed = 0
    empty = 0
    failed_assets = []

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

            path = save_price_history(df, OUTPUT_PATH)

            print(f"✅ Salvo em: {path}")
            success += 1

        except Exception as e:
            print(f"❌ Erro em {asset}: {e}")
            failed += 1
            failed_assets.append(f"{asset} ({ticker})")

    print("\n📦 Resumo:")
    print(f"✔ Sucesso: {success}")
    print(f"⚠️ Sem dados: {empty}")
    print(f"❌ Erros: {failed}")

    if failed_assets:
        print("Ativos com falha:")
        for failed_asset in failed_assets:
            print(f"- {failed_asset}")

    if failed > 0:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

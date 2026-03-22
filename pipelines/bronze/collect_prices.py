# Pipeline de ingestão de dados

from pathlib import Path
from src.collect.reader import read_assets
from src.collect.fetcher import fetch_price_history
from src.collect.writer import save_price_history



ASSETS_FILE = Path("config/assets.txt")
OUTPUT_PATH = "data/bronze/prices"


def main():
    print("🚀 Iniciando coleta Bronze...")

    assets = read_assets(ASSETS_FILE)
    print(f"Total de ativos: {len(assets)}")

    success = 0
    failed = 0
    empty = 0

    for asset_info in assets:
        asset = asset_info["asset"]
        ticker = asset_info["ticker"]

        print(f"\n📊 Processando: {asset} ({ticker})")

        try:
            df = fetch_price_history(asset_info)

            if df.empty:
                print(f"⚠️ Sem dados para {asset}")
                empty += 1
                continue

            path = save_price_history(df, OUTPUT_PATH)

            print(f"✅ Salvo em: {path}")
            success += 1

        except Exception as e:
            print(f"❌ Erro em {asset}: {e}")
            failed += 1

    print("\n📦 Resumo:")
    print(f"✔ Sucesso: {success}")
    print(f"⚠️ Sem dados: {empty}")
    print(f"❌ Erros: {failed}")


if __name__ == "__main__":
    main()
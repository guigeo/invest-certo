from pathlib import Path
from typing import Union


def read_assets(file_path: Union[str, Path]) -> list[dict]:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    assets = []
    seen_assets = set()
    seen_tickers = set()

    with path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            parts = [part.strip() for part in line.split("|")]

            if len(parts) != 4:
                raise ValueError(
                    f"Linha inválida no arquivo de ativos "
                    f"(linha {line_number}): {line}"
                )

            asset, asset_type, source, ticker = parts

            if not all([asset, asset_type, source, ticker]):
                raise ValueError(
                    f"Linha inválida no arquivo de ativos "
                    f"(linha {line_number}): campos vazios não são permitidos."
                )

            if asset in seen_assets:
                raise ValueError(
                    f"Ativo duplicado no arquivo de ativos "
                    f"(linha {line_number}): {asset}"
                )

            if ticker in seen_tickers:
                raise ValueError(
                    f"Ticker duplicado no arquivo de ativos "
                    f"(linha {line_number}): {ticker}"
                )

            assets.append(
                {
                    "asset": asset,
                    "type": asset_type,
                    "source": source,
                    "ticker": ticker,
                }
            )
            seen_assets.add(asset)
            seen_tickers.add(ticker)

    if not assets:
        raise ValueError("Nenhum ativo válido encontrado no arquivo.")

    return assets

import pandas as pd
import yfinance as yf


REQUIRED_COLUMNS = {
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


def fetch_price_history(asset_info: dict) -> pd.DataFrame:
    ticker = asset_info["ticker"]
    asset = asset_info["asset"]

    df = yf.download(
        tickers=ticker,
        start="2015-01-01",
        progress=False
    )

    if df.empty:
        return pd.DataFrame()

    # 🔹 1. Se tiver MultiIndex, remove o nível do ticker
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # 🔹 2. Reset index (Date vira coluna)
    df = df.reset_index()

    # 🔹 3. Padroniza nomes
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    # 🔹 4. Adiciona metadata
    df["asset"] = asset
    df["ticker"] = ticker

    # 🔹 5. Remove nome das colunas (evita problemas futuros)
    df.columns.name = None

    missing_columns = REQUIRED_COLUMNS.difference(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Schema inválido retornado para {asset} ({ticker}). "
            f"Colunas ausentes: {missing}"
        )

    # 🔹 6. Ordena
    df = df.sort_values("date")

    return df

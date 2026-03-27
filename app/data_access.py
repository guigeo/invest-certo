from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
QUERIES_DIR = REPO_ROOT / "queries" / "gold"


def _data_root() -> Path:
    return Path.cwd()


def _asset_features_dir() -> Path:
    return _data_root() / "data" / "gold" / "asset_features"


def _ranking_snapshot_dir() -> Path:
    return _data_root() / "data" / "gold" / "ranking_snapshot"


class GoldDataNotReadyError(FileNotFoundError):
    """Raised when Gold parquet outputs are not available for the dashboard."""


def _dataset_has_parquet(dataset_dir: Path) -> bool:
    return dataset_dir.exists() and any(dataset_dir.rglob("*.parquet"))


def ensure_gold_data_ready() -> None:
    missing = []
    asset_features_dir = _asset_features_dir()
    ranking_snapshot_dir = _ranking_snapshot_dir()
    if not _dataset_has_parquet(asset_features_dir):
        missing.append(str(asset_features_dir.relative_to(_data_root())))
    if not _dataset_has_parquet(ranking_snapshot_dir):
        missing.append(str(ranking_snapshot_dir.relative_to(_data_root())))

    if missing:
        raise GoldDataNotReadyError(
            "Dados da Gold nao encontrados para o dashboard. Gere a Silver e a Gold antes de abrir o app. "
            f"Datasets ausentes: {', '.join(missing)}"
        )


def _load_query(query_name: str) -> str:
    query_path = QUERIES_DIR / query_name
    if not query_path.exists():
        raise FileNotFoundError(f"Query nao encontrada: {query_path}")
    return query_path.read_text(encoding="utf-8")


def _run_query(sql: str) -> pd.DataFrame:
    ensure_gold_data_ready()
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        if "date" in column:
            try:
                normalized[column] = pd.to_datetime(normalized[column])
            except (TypeError, ValueError):
                continue
    return normalized


def load_latest_recommendations() -> pd.DataFrame:
    df = _run_query(_load_query("latest_recommendations.sql"))
    return _normalize_date_columns(df)


def load_price_history(asset: str | None = None) -> pd.DataFrame:
    df = _run_query(_load_query("price_history_panel.sql"))
    df = _normalize_date_columns(df)
    if asset is not None:
        df = df[df["asset"] == asset].copy()
    return df.sort_values(["asset", "reference_date"]).reset_index(drop=True)


def load_ranking_history(asset: str | None = None) -> pd.DataFrame:
    df = _run_query(_load_query("ranking_history.sql"))
    df = _normalize_date_columns(df)
    if asset is not None:
        df = df[df["asset"] == asset].copy()
    return df.sort_values(["asset", "reference_date"]).reset_index(drop=True)


def load_market_overview() -> pd.DataFrame:
    df = _run_query(_load_query("market_overview.sql"))
    return _normalize_date_columns(df).sort_values("reference_date").reset_index(drop=True)

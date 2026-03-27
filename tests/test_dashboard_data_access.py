from pathlib import Path

import pandas as pd
import pytest

from app.data_access import (
    GoldDataNotReadyError,
    ensure_gold_data_ready,
    load_latest_recommendations,
    load_market_overview,
    load_price_history,
    load_ranking_history,
)
from pipelines.gold.build_features import main as build_gold_main
from tests.test_gold_build_features import (
    create_asset_daily_status,
    create_prices_clean,
    write_dataset,
)


def materialize_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prices_clean = create_prices_clean()
    asset_daily_status = create_asset_daily_status(prices_clean)

    silver_prices_dir = tmp_path / "data" / "silver" / "prices_clean"
    silver_status_dir = tmp_path / "data" / "silver" / "asset_daily_status"
    write_dataset(prices_clean, silver_prices_dir, "date")
    write_dataset(asset_daily_status, silver_status_dir, "date")

    monkeypatch.chdir(tmp_path)
    exit_code = build_gold_main()
    assert exit_code == 0


def test_ensure_gold_data_ready_raises_when_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(GoldDataNotReadyError):
        ensure_gold_data_ready()


def test_dashboard_data_access_loads_latest_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize_gold(tmp_path, monkeypatch)

    recommendations = load_latest_recommendations()
    assert recommendations["reference_date"].nunique() == 1
    assert recommendations.iloc[0]["asset"] == "ALFA3"


def test_dashboard_data_access_filters_history_by_asset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize_gold(tmp_path, monkeypatch)

    price_history = load_price_history("ALFA3")
    ranking_history = load_ranking_history("ALFA3")

    assert price_history["asset"].eq("ALFA3").all()
    assert ranking_history["asset"].eq("ALFA3").all()
    assert pd.api.types.is_datetime64_any_dtype(price_history["reference_date"])
    assert pd.api.types.is_datetime64_any_dtype(ranking_history["reference_date"])


def test_dashboard_data_access_loads_market_overview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize_gold(tmp_path, monkeypatch)

    market_overview = load_market_overview()

    assert not market_overview.empty
    assert "eligible_asset_count" in market_overview.columns
    assert "avg_return_90d" in market_overview.columns
    assert pd.api.types.is_datetime64_any_dtype(market_overview["reference_date"])


def test_streamlit_app_module_imports() -> None:
    import app.streamlit_app  # noqa: F401

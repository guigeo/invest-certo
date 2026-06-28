from pathlib import Path

import pandas as pd
import pytest

from app.ai.prompts import AGENT_INSTRUCTIONS
from app.ai.tools import (
    compare_assets,
    detect_recent_drop,
    get_asset_history_summary,
    get_latest_asset_snapshot,
    list_monitored_assets,
)
from tests.test_dashboard_data_access import materialize_gold


def write_assets_config(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "assets.txt").write_text(
        "\n".join(
            [
                "# asset|type|source|ticker",
                "ALFA3|stock|yahoo|ALFA3.SA",
                "BETA3|stock|yahoo|BETA3.SA",
                "CURTA3|stock|yahoo|CURTA3.SA",
            ]
        ),
        encoding="utf-8",
    )


def prepare_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_assets_config(tmp_path)
    materialize_gold(tmp_path, monkeypatch)


def test_ai_tools_list_monitored_assets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)

    result = list_monitored_assets()

    assert result["status"] == "ok"
    assert result["asset_count"] == 3
    assert result["assets"][0]["asset"] == "ALFA3"
    assert result["latest_reference_date"] is not None


def test_ai_tools_get_latest_asset_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)

    result = get_latest_asset_snapshot("alfa3")

    assert result["status"] == "ok"
    assert result["snapshot"]["asset"] == "ALFA3"
    assert result["snapshot"]["rank_position"] == 1
    assert result["snapshot"]["reference_date"] is not None


def test_ai_tools_reject_unknown_asset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)

    result = get_latest_asset_snapshot("NAOEXISTE")

    assert result["status"] == "error"
    assert result["error_code"] == "asset_not_monitored"


def test_ai_tools_history_summary_uses_gold_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)

    result = get_asset_history_summary("ALFA3", window_days=365)

    assert result["status"] == "ok"
    assert result["asset"] == "ALFA3"
    assert result["observations"] > 200
    assert result["period_return"] > 0
    assert result["latest_metrics"]["return_90d"] is not None


def test_ai_tools_detect_recent_drop_with_controlled_gold_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)
    features_paths = list((tmp_path / "data" / "gold" / "asset_features").rglob("*.parquet"))
    all_features = pd.concat([pd.read_parquet(path) for path in features_paths], ignore_index=True)
    latest_date = all_features["reference_date"].max()
    features_path = next(
        path for path in features_paths if latest_date in set(pd.read_parquet(path)["reference_date"])
    )
    features = pd.read_parquet(features_path)
    mask = (features["asset"] == "ALFA3") & (features["reference_date"] == latest_date)
    features.loc[mask, "adj_close"] = features.loc[mask, "adj_close"] * 0.70
    features.loc[mask, "close"] = features.loc[mask, "close"] * 0.70
    features.to_parquet(features_path, index=False)

    result = detect_recent_drop("ALFA3", window_days=30, threshold_pct=0.10)

    assert result["status"] == "ok"
    assert result["drop_detected"] is True
    assert result["max_drawdown_in_window"] <= -0.10


def test_ai_tools_compare_assets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepare_gold(tmp_path, monkeypatch)

    result = compare_assets(["BETA3", "ALFA3"])

    assert result["status"] == "ok"
    assert [row["asset"] for row in result["comparison"]] == ["ALFA3", "BETA3"]
    assert result["comparison"][0]["score"] >= result["comparison"][1]["score"]


def test_ai_tools_report_gold_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_assets_config(tmp_path)
    monkeypatch.chdir(tmp_path)

    result = list_monitored_assets()

    assert result["status"] == "error"
    assert result["error_code"] == "gold_data_not_ready"


def test_agent_prompt_declares_no_external_sources() -> None:
    instructions = " ".join(AGENT_INSTRUCTIONS).lower()

    assert "não use internet" in instructions
    assert "documentos externos" in instructions

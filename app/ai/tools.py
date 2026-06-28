from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from app.data_access import (
    GoldDataNotReadyError,
    load_latest_recommendations,
    load_price_history,
    load_ranking_history,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


SIGNAL_LABELS = {
    "trend_strength": "Tendencia forte",
    "risk_adjusted_quality": "Boa relacao retorno/risco",
    "recovery_setup": "Possivel recuperacao",
    "balanced_setup": "Perfil equilibrado",
    "risk_control": "Risco controlado",
    "price_above_moving_averages": "Preco acima das medias",
    "deep_drawdown": "Queda relevante",
    "watchlist": "Acompanhar",
}


def _json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _row_to_dict(row: pd.Series) -> dict[str, Any]:
    return {column: _json_value(value) for column, value in row.items()}


def _normalize_asset(asset: str) -> str:
    return asset.strip().upper()


def _error(code: str, message: str) -> dict[str, Any]:
    return {"status": "error", "error_code": code, "message": message}


def _load_assets_catalog() -> dict[str, dict[str, str]]:
    cwd_assets_file = Path.cwd() / "config" / "assets.txt"
    assets_file = cwd_assets_file if cwd_assets_file.exists() else REPO_ROOT / "config" / "assets.txt"
    if not assets_file.exists():
        return {}

    catalog: dict[str, dict[str, str]] = {}
    for line in assets_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) != 4:
            continue
        asset, asset_type, source, ticker = parts
        catalog[_normalize_asset(asset)] = {
            "asset": _normalize_asset(asset),
            "asset_type": asset_type,
            "source": source,
            "ticker": ticker,
        }
    return catalog


def _load_latest_recommendations_safe() -> pd.DataFrame:
    try:
        return load_latest_recommendations()
    except GoldDataNotReadyError as exc:
        raise GoldDataNotReadyError(str(exc)) from exc


def _validate_asset(asset: str) -> tuple[str | None, dict[str, Any] | None]:
    normalized = _normalize_asset(asset)
    catalog = _load_assets_catalog()
    if catalog and normalized not in catalog:
        return None, _error(
            "asset_not_monitored",
            f"O ativo {normalized} não está em config/assets.txt.",
        )

    try:
        recommendations = _load_latest_recommendations_safe()
    except GoldDataNotReadyError as exc:
        return None, _error("gold_data_not_ready", str(exc))

    available_assets = set(recommendations["asset"].str.upper())
    if normalized not in available_assets:
        return None, _error(
            "asset_without_gold_data",
            f"O ativo {normalized} está monitorado, mas não tem dados analíticos disponíveis.",
        )
    return normalized, None


def list_monitored_assets() -> dict[str, Any]:
    """List monitored assets from config/assets.txt and the latest analytical snapshot.

    Returns:
        dict: Structured data with monitored assets and latest analytical availability.
    """
    catalog = _load_assets_catalog()
    try:
        recommendations = _load_latest_recommendations_safe()
    except GoldDataNotReadyError as exc:
        return _error("gold_data_not_ready", str(exc))

    gold_assets = recommendations[["asset", "ticker", "asset_type", "eligibility_status"]].copy()
    gold_assets["asset"] = gold_assets["asset"].str.upper()
    gold_by_asset = {
        row["asset"]: row for _, row in gold_assets.iterrows()
    }
    latest_reference_date = recommendations["reference_date"].max()
    assets = []
    listed_assets = sorted(catalog.keys()) if catalog else sorted(gold_by_asset.keys())
    for asset in listed_assets:
        row = gold_by_asset.get(asset)
        config_entry = catalog.get(asset, {})
        assets.append(
            {
                "asset": asset,
                "ticker": _json_value(row["ticker"]) if row is not None else config_entry.get("ticker"),
                "asset_type": _json_value(row["asset_type"]) if row is not None else config_entry.get("asset_type"),
                "source": config_entry.get("source"),
                "eligibility_status": _json_value(row["eligibility_status"]) if row is not None else None,
                "in_config": asset in catalog if catalog else None,
                "has_latest_gold_snapshot": row is not None,
            }
        )

    return {
        "status": "ok",
        "latest_reference_date": _json_value(latest_reference_date),
        "asset_count": len(assets),
        "assets": assets,
    }


def get_latest_asset_snapshot(asset: str) -> dict[str, Any]:
    """Get the latest ranking and feature snapshot for one monitored asset.

    Args:
        asset (str): Monitored asset code, for example PETR4.

    Returns:
        dict: Structured latest snapshot with score, ranking, returns and risk metrics.
    """
    normalized, validation_error = _validate_asset(asset)
    if validation_error:
        return validation_error

    recommendations = _load_latest_recommendations_safe()
    row = recommendations[recommendations["asset"].str.upper() == normalized].iloc[0]
    return {"status": "ok", "snapshot": _row_to_dict(row)}


def get_asset_history_summary(asset: str, window_days: int = 365) -> dict[str, Any]:
    """Summarize historical analytical metrics for a monitored asset in a time window.

    Args:
        asset (str): Monitored asset code, for example PETR4.
        window_days (int): Calendar-day window ending at the latest analytical date.

    Returns:
        dict: Structured history summary with returns, volatility and drawdown.
    """
    normalized, validation_error = _validate_asset(asset)
    if validation_error:
        return validation_error

    window_days = max(int(window_days), 1)
    try:
        history = load_price_history(normalized)
    except GoldDataNotReadyError as exc:
        return _error("gold_data_not_ready", str(exc))

    if history.empty:
        return _error("asset_without_history", f"Não há histórico analítico para {normalized}.")

    history = history.sort_values("reference_date").reset_index(drop=True)
    latest_date = history["reference_date"].max()
    window_start = latest_date - pd.Timedelta(days=window_days)
    window = history[history["reference_date"] >= window_start].copy()
    if window.empty:
        window = history.tail(1).copy()

    first = window.iloc[0]
    latest = window.iloc[-1]
    period_return = latest["adj_close"] / first["adj_close"] - 1
    high_price = window["adj_close"].max()
    low_price = window["adj_close"].min()
    max_drawdown_in_window = (window["adj_close"] / window["adj_close"].cummax() - 1).min()

    return {
        "status": "ok",
        "asset": normalized,
        "window_days": window_days,
        "reference_start_date": _json_value(first["reference_date"]),
        "reference_end_date": _json_value(latest["reference_date"]),
        "observations": int(len(window)),
        "start_adj_close": _json_value(first["adj_close"]),
        "latest_adj_close": _json_value(latest["adj_close"]),
        "period_return": _json_value(period_return),
        "high_adj_close": _json_value(high_price),
        "low_adj_close": _json_value(low_price),
        "max_drawdown_in_window": _json_value(max_drawdown_in_window),
        "latest_metrics": {
            "return_30d": _json_value(latest.get("return_30d")),
            "return_90d": _json_value(latest.get("return_90d")),
            "return_252d": _json_value(latest.get("return_252d")),
            "volatility_30d": _json_value(latest.get("volatility_30d")),
            "drawdown_252d": _json_value(latest.get("drawdown_252d")),
            "distance_to_ma20": _json_value(latest.get("distance_to_ma20")),
            "distance_to_ma90": _json_value(latest.get("distance_to_ma90")),
            "price_vs_52w_high": _json_value(latest.get("price_vs_52w_high")),
            "price_vs_52w_low": _json_value(latest.get("price_vs_52w_low")),
            "momentum_bucket": _json_value(latest.get("momentum_bucket")),
            "risk_bucket": _json_value(latest.get("risk_bucket")),
        },
    }


def detect_recent_drop(
    asset: str,
    window_days: int = 30,
    threshold_pct: float = 0.10,
) -> dict[str, Any]:
    """Detect whether a monitored asset had a relevant recent price drop.

    Args:
        asset (str): Monitored asset code, for example PETR4.
        window_days (int): Calendar-day window ending at the latest analytical date.
        threshold_pct (float): Drop threshold as decimal (0.10) or percent (10).

    Returns:
        dict: Structured drop analysis for the selected window.
    """
    normalized, validation_error = _validate_asset(asset)
    if validation_error:
        return validation_error

    threshold = float(threshold_pct)
    if threshold > 1:
        threshold = threshold / 100
    threshold = abs(threshold)
    summary = get_asset_history_summary(normalized, window_days)
    if summary.get("status") != "ok":
        return summary

    period_return = summary["period_return"]
    max_drawdown = summary["max_drawdown_in_window"]
    period_drop_detected = period_return is not None and period_return <= -threshold
    drawdown_drop_detected = max_drawdown is not None and max_drawdown <= -threshold

    return {
        "status": "ok",
        "asset": normalized,
        "window_days": summary["window_days"],
        "threshold": threshold,
        "reference_start_date": summary["reference_start_date"],
        "reference_end_date": summary["reference_end_date"],
        "period_return": period_return,
        "max_drawdown_in_window": max_drawdown,
        "drop_detected": bool(period_drop_detected or drawdown_drop_detected),
        "drop_reason": {
            "period_return_breached": bool(period_drop_detected),
            "max_drawdown_breached": bool(drawdown_drop_detected),
        },
    }


def compare_assets(assets: list[str]) -> dict[str, Any]:
    """Compare monitored assets using the latest ranking and metrics.

    Args:
        assets (list[str]): Asset codes to compare.

    Returns:
        dict: Structured comparison ordered by current ranking position.
    """
    if isinstance(assets, str):
        assets = [asset.strip() for asset in assets.split(",")]
    normalized_assets = [_normalize_asset(asset) for asset in assets if asset.strip()]
    if not normalized_assets:
        return _error("missing_assets", "Informe ao menos um ativo monitorado para comparar.")

    rows = []
    errors = []
    for asset in normalized_assets:
        snapshot = get_latest_asset_snapshot(asset)
        if snapshot.get("status") != "ok":
            errors.append({"asset": asset, "error": snapshot})
            continue
        row = snapshot["snapshot"]
        rows.append(
            {
                "asset": row["asset"],
                "ticker": row["ticker"],
                "reference_date": row["reference_date"],
                "rank_position": row["rank_position"],
                "score": row["score"],
                "eligibility_status": row["eligibility_status"],
                "return_90d": row["return_90d"],
                "volatility_30d": row["volatility_30d"],
                "drawdown_252d": row["drawdown_252d"],
                "rank_delta_7d": row["rank_delta_7d"],
                "rank_delta_30d": row["rank_delta_30d"],
                "momentum_bucket": row["momentum_bucket"],
                "risk_bucket": row["risk_bucket"],
                "primary_signal": row["primary_signal"],
                "secondary_signal": row["secondary_signal"],
            }
        )

    rows = sorted(rows, key=lambda item: item["rank_position"])
    return {
        "status": "ok" if rows else "error",
        "assets_requested": normalized_assets,
        "comparison": rows,
        "errors": errors,
    }


def explain_asset_ranking(asset: str) -> dict[str, Any]:
    """Explain the current ranking position of a monitored asset using analytical metrics.

    Args:
        asset (str): Monitored asset code, for example PETR4.

    Returns:
        dict: Structured explanation ingredients for the agent response.
    """
    snapshot = get_latest_asset_snapshot(asset)
    if snapshot.get("status") != "ok":
        return snapshot

    row = snapshot["snapshot"]
    return {
        "status": "ok",
        "asset": row["asset"],
        "reference_date": row["reference_date"],
        "rank_position": row["rank_position"],
        "score": row["score"],
        "ranking_bucket": row["ranking_bucket"],
        "eligibility_status": row["eligibility_status"],
        "primary_signal": row["primary_signal"],
        "primary_signal_label": SIGNAL_LABELS.get(row["primary_signal"], row["primary_signal"]),
        "secondary_signal": row["secondary_signal"],
        "secondary_signal_label": SIGNAL_LABELS.get(row["secondary_signal"], row["secondary_signal"]),
        "evidence": {
            "return_90d": row["return_90d"],
            "volatility_30d": row["volatility_30d"],
            "drawdown_252d": row["drawdown_252d"],
            "trend_ratio": row["trend_ratio"],
            "distance_to_ma20": row["distance_to_ma20"],
            "distance_to_ma90": row["distance_to_ma90"],
            "price_vs_52w_high": row["price_vs_52w_high"],
            "price_vs_52w_low": row["price_vs_52w_low"],
            "rank_delta_7d": row["rank_delta_7d"],
            "rank_delta_30d": row["rank_delta_30d"],
            "score_delta_7d": row["score_delta_7d"],
            "score_delta_30d": row["score_delta_30d"],
            "momentum_bucket": row["momentum_bucket"],
            "risk_bucket": row["risk_bucket"],
        },
    }

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.data_access import (
    GoldDataNotReadyError,
    load_latest_recommendations,
    load_market_overview,
    load_price_history,
    load_ranking_history,
)


@st.cache_data(show_spinner=False)
def get_latest_recommendations() -> pd.DataFrame:
    return load_latest_recommendations()


@st.cache_data(show_spinner=False)
def get_price_history(asset: str | None = None) -> pd.DataFrame:
    return load_price_history(asset)


@st.cache_data(show_spinner=False)
def get_ranking_history(asset: str | None = None) -> pd.DataFrame:
    return load_ranking_history(asset)


@st.cache_data(show_spinner=False)
def get_market_overview() -> pd.DataFrame:
    return load_market_overview()


def _format_pct(series: pd.Series) -> pd.Series:
    return series.map(lambda value: f"{value:.2%}" if pd.notna(value) else "-")


def _format_number(series: pd.Series, digits: int = 2) -> pd.Series:
    return series.map(lambda value: f"{value:.{digits}f}" if pd.notna(value) else "-")


def _delta_label(value: float | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:+.0f}"


def _bucket_color(bucket: str) -> str:
    colors = {
        "top_3": "#0f766e",
        "top_5": "#2563eb",
        "middle": "#b45309",
        "tail": "#6b7280",
    }
    return colors.get(bucket, "#6b7280")


def _render_top_cards(recommendations: pd.DataFrame) -> None:
    top_five = recommendations.head(5)
    st.subheader("Recomendacao do momento")
    card_columns = st.columns(len(top_five))
    for column, (_, row) in zip(card_columns, top_five.iterrows()):
        with column:
            st.markdown(
                f"""
                <div style="background:{_bucket_color(row['ranking_bucket'])};padding:1rem;border-radius:1rem;color:white;min-height:180px;">
                    <div style="font-size:0.9rem;opacity:0.8;">Rank #{int(row['rank_position'])}</div>
                    <div style="font-size:1.5rem;font-weight:700;">{row['asset']}</div>
                    <div style="margin-top:0.4rem;">Score {row['score']:.2f}</div>
                    <div>{row['primary_signal'].replace('_', ' ')}</div>
                    <div>{row['secondary_signal'].replace('_', ' ')}</div>
                    <div style="margin-top:0.6rem;font-size:0.9rem;">Retorno 90d {row['return_90d']:.2%}</div>
                    <div style="font-size:0.9rem;">Vol 30d {row['volatility_30d']:.2%}</div>
                    <div style="font-size:0.9rem;">Drawdown {row['drawdown_252d']:.2%}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _build_scatter_plot(df: pd.DataFrame) -> go.Figure:
    scatter = px.scatter(
        df,
        x="volatility_30d",
        y="return_90d",
        color="ranking_bucket",
        size=df["is_top_pick"].map({True: 28, False: 16}),
        hover_name="asset",
        hover_data={
            "score": ":.2f",
            "rank_position": True,
            "volatility_30d": ":.2%",
            "return_90d": ":.2%",
            "is_top_pick": False,
        },
        color_discrete_map={
            "top_3": "#0f766e",
            "top_5": "#2563eb",
            "middle": "#b45309",
            "tail": "#6b7280",
        },
        labels={
            "volatility_30d": "Volatilidade 30d",
            "return_90d": "Retorno 90d",
            "ranking_bucket": "Bucket",
        },
    )
    scatter.update_layout(
        height=440,
        margin=dict(l=20, r=20, t=30, b=20),
        legend_title_text="Bucket do ranking",
    )
    scatter.update_xaxes(tickformat=".0%")
    scatter.update_yaxes(tickformat=".0%")
    return scatter


def _build_price_chart(price_history: pd.DataFrame, asset: str) -> go.Figure:
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=price_history["reference_date"],
            y=price_history["adj_close"],
            name="Preco ajustado",
            line=dict(color="#111827", width=2.5),
        )
    )
    chart.add_trace(
        go.Scatter(
            x=price_history["reference_date"],
            y=price_history["ma_20"],
            name="MA 20",
            line=dict(color="#2563eb", width=1.8),
        )
    )
    chart.add_trace(
        go.Scatter(
            x=price_history["reference_date"],
            y=price_history["ma_90"],
            name="MA 90",
            line=dict(color="#0f766e", width=1.8),
        )
    )
    chart.update_layout(
        title=f"Preco e tendencia de {asset}",
        height=430,
        margin=dict(l=20, r=20, t=55, b=20),
        legend_orientation="h",
    )
    return chart


def _build_risk_chart(price_history: pd.DataFrame, asset: str) -> go.Figure:
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=price_history["reference_date"],
            y=price_history["drawdown_252d"],
            name="Drawdown 252d",
            line=dict(color="#dc2626", width=2),
            yaxis="y",
        )
    )
    chart.add_trace(
        go.Scatter(
            x=price_history["reference_date"],
            y=price_history["volatility_30d"],
            name="Volatilidade 30d",
            line=dict(color="#7c3aed", width=2),
            yaxis="y2",
        )
    )
    chart.update_layout(
        title=f"Risco e drawdown de {asset}",
        height=360,
        margin=dict(l=20, r=20, t=55, b=20),
        yaxis=dict(title="Drawdown", tickformat=".0%"),
        yaxis2=dict(
            title="Volatilidade",
            tickformat=".0%",
            overlaying="y",
            side="right",
        ),
        legend_orientation="h",
    )
    return chart


def _build_market_chart(market_overview: pd.DataFrame) -> go.Figure:
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=market_overview["reference_date"],
            y=market_overview["eligible_asset_count"],
            name="Ativos elegiveis",
            line=dict(color="#111827", width=2.5),
        )
    )
    chart.add_trace(
        go.Scatter(
            x=market_overview["reference_date"],
            y=market_overview["positive_trend_count"],
            name="Tendencia positiva",
            line=dict(color="#0f766e", width=2),
        )
    )
    chart.add_trace(
        go.Scatter(
            x=market_overview["reference_date"],
            y=market_overview["avg_return_90d"],
            name="Media retorno 90d",
            line=dict(color="#2563eb", width=2),
            yaxis="y2",
        )
    )
    chart.add_trace(
        go.Scatter(
            x=market_overview["reference_date"],
            y=market_overview["avg_volatility_30d"],
            name="Media volatilidade 30d",
            line=dict(color="#b45309", width=2),
            yaxis="y2",
        )
    )
    chart.update_layout(
        title="Panorama do mercado monitorado",
        height=400,
        margin=dict(l=20, r=20, t=55, b=20),
        yaxis=dict(title="Contagem"),
        yaxis2=dict(
            title="Metricas percentuais",
            tickformat=".0%",
            overlaying="y",
            side="right",
        ),
        legend_orientation="h",
    )
    return chart


def _build_ranking_history_chart(ranking_history: pd.DataFrame, asset: str) -> go.Figure:
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=ranking_history["reference_date"],
            y=ranking_history["rank_position"],
            name="Posicao no ranking",
            line=dict(color="#111827", width=2.5),
        )
    )
    chart.add_trace(
        go.Scatter(
            x=ranking_history["reference_date"],
            y=ranking_history["score"],
            name="Score",
            line=dict(color="#2563eb", width=2),
            yaxis="y2",
        )
    )
    chart.update_layout(
        title=f"Historico de ranking de {asset}",
        height=360,
        margin=dict(l=20, r=20, t=55, b=20),
        yaxis=dict(title="Rank", autorange="reversed"),
        yaxis2=dict(title="Score", overlaying="y", side="right"),
        legend_orientation="h",
    )
    return chart


def main() -> None:
    st.title("Invest Certo")
    st.caption(
        "Dashboard para apoiar a decisao de aporte mensal com ranking, tendencia, risco e leitura do mercado."
    )

    try:
        recommendations = get_latest_recommendations()
        market_overview = get_market_overview()
    except GoldDataNotReadyError as exc:
        st.warning(str(exc))
        st.code(
            "\n".join(
                [
                    "uv run python pipelines/silver/transform_prices.py",
                    "uv run python pipelines/gold/build_features.py",
                    "uv run streamlit run app/streamlit_app.py",
                ]
            ),
            language="bash",
        )
        st.stop()

    latest_date = recommendations["reference_date"].max()
    latest_snapshot = recommendations[recommendations["reference_date"] == latest_date].copy()
    top_pick = latest_snapshot.loc[latest_snapshot["is_top_pick"]].head(1)
    top_pick_asset = top_pick["asset"].iloc[0] if not top_pick.empty else latest_snapshot.iloc[0]["asset"]
    eligible_asset_count = int((latest_snapshot["eligibility_status"] == "eligible").sum())

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Snapshot mais recente", latest_date.strftime("%d/%m/%Y"))
    metric_2.metric("Ativos elegiveis", eligible_asset_count)
    metric_3.metric("Melhor ativo atual", top_pick_asset)

    show_ineligible = st.toggle("Mostrar ativos inelegiveis", value=False)
    asset_types = sorted(latest_snapshot["asset_type"].dropna().unique().tolist())
    selected_types = st.multiselect(
        "Filtrar por tipo de ativo",
        options=asset_types,
        default=asset_types,
    )

    filtered_snapshot = latest_snapshot[latest_snapshot["asset_type"].isin(selected_types)].copy()
    if not show_ineligible:
        filtered_snapshot = filtered_snapshot[filtered_snapshot["eligibility_status"] == "eligible"].copy()
    if filtered_snapshot.empty:
        st.info("Nenhum ativo atende aos filtros atuais.")
        st.stop()

    _render_top_cards(filtered_snapshot)

    st.markdown("### Tabela executiva")
    executive_table = filtered_snapshot[
        [
            "rank_position",
            "asset",
            "score",
            "ranking_bucket",
            "primary_signal",
            "secondary_signal",
            "return_90d",
            "volatility_30d",
            "drawdown_252d",
            "momentum_bucket",
            "risk_bucket",
            "rank_delta_7d",
            "rank_delta_30d",
        ]
    ].copy()
    executive_table["score"] = _format_number(executive_table["score"])
    executive_table["return_90d"] = _format_pct(executive_table["return_90d"])
    executive_table["volatility_30d"] = _format_pct(executive_table["volatility_30d"])
    executive_table["drawdown_252d"] = _format_pct(executive_table["drawdown_252d"])
    executive_table["rank_delta_7d"] = executive_table["rank_delta_7d"].map(_delta_label)
    executive_table["rank_delta_30d"] = executive_table["rank_delta_30d"].map(_delta_label)
    st.dataframe(executive_table, use_container_width=True, hide_index=True)

    st.markdown("### Comparacao rapida entre ativos")
    scatter_col, summary_col = st.columns([1.4, 1.0])
    with scatter_col:
        st.plotly_chart(_build_scatter_plot(filtered_snapshot), use_container_width=True)
    with summary_col:
        summary_table = filtered_snapshot[
            ["rank_position", "asset", "score", "eligibility_status", "ranking_bucket"]
        ].copy()
        summary_table["score"] = _format_number(summary_table["score"])
        st.dataframe(summary_table, use_container_width=True, hide_index=True)

    selected_asset = st.selectbox(
        "Ativo para analise detalhada",
        options=filtered_snapshot["asset"].tolist(),
        index=max(filtered_snapshot["asset"].tolist().index(top_pick_asset), 0)
        if top_pick_asset in filtered_snapshot["asset"].tolist()
        else 0,
    )

    price_history = get_price_history(selected_asset)
    ranking_history = get_ranking_history(selected_asset)
    latest_asset_row = filtered_snapshot[filtered_snapshot["asset"] == selected_asset].iloc[0]

    st.markdown("### Analise detalhada do ativo")
    detail_metric_cols = st.columns(4)
    detail_metric_cols[0].metric("Score atual", f"{latest_asset_row['score']:.2f}")
    detail_metric_cols[1].metric("Rank atual", f"#{int(latest_asset_row['rank_position'])}")
    detail_metric_cols[2].metric("Rank delta 7d", _delta_label(latest_asset_row["rank_delta_7d"]))
    detail_metric_cols[3].metric("Rank delta 30d", _delta_label(latest_asset_row["rank_delta_30d"]))

    signal_metric_cols = st.columns(4)
    signal_metric_cols[0].metric("Momentum", str(latest_asset_row["momentum_bucket"]).replace("_", " "))
    signal_metric_cols[1].metric("Risco", str(latest_asset_row["risk_bucket"]).replace("_", " "))
    signal_metric_cols[2].metric(
        "Distancia MA20",
        f"{latest_asset_row['distance_to_ma20']:.2%}" if pd.notna(latest_asset_row["distance_to_ma20"]) else "-",
    )
    signal_metric_cols[3].metric(
        "Distancia MA90",
        f"{latest_asset_row['distance_to_ma90']:.2%}" if pd.notna(latest_asset_row["distance_to_ma90"]) else "-",
    )

    price_col, risk_col = st.columns([1.45, 1.0])
    with price_col:
        st.plotly_chart(_build_price_chart(price_history, selected_asset), use_container_width=True)
    with risk_col:
        st.plotly_chart(_build_risk_chart(price_history, selected_asset), use_container_width=True)

    st.markdown("### Visao de mercado")
    overview_col, history_col = st.columns([1.35, 1.0])
    with overview_col:
        st.plotly_chart(_build_market_chart(market_overview), use_container_width=True)
    with history_col:
        st.plotly_chart(_build_ranking_history_chart(ranking_history, selected_asset), use_container_width=True)


if __name__ == "__main__":
    st.set_page_config(
        page_title="Invest Certo Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
    )
    main()

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


def _rank_delta_text(value: float | None) -> str:
    if pd.isna(value):
        return "Sem histórico"
    rounded = int(value)
    if rounded > 0:
        return f"Melhorou {rounded}"
    if rounded < 0:
        return f"Piorou {abs(rounded)}"
    return "Estável"


def _translate_value(value: object, translations: dict[str, str]) -> str:
    if pd.isna(value):
        return "-"
    text = str(value)
    return translations.get(text, text.replace("_", " ").title())


RANKING_BUCKET_LABELS = {
    "top_3": "Top 3",
    "top_5": "Top 5",
    "middle": "Meio do ranking",
    "tail": "Fim do ranking",
}

MOMENTUM_LABELS = {
    "strong": "Forte",
    "neutral": "Neutro",
    "weak": "Fraco",
}

RISK_LABELS = {
    "low": "Baixo",
    "medium": "Médio",
    "high": "Alto",
}

ELIGIBILITY_LABELS = {
    "eligible": "Elegível",
    "insufficient_history": "Histórico insuficiente",
    "calendar_gap_anomaly": "Falha no calendário",
    "volume_missing": "Volume ausente",
    "invalid_price": "Preço inválido",
}

SIGNAL_LABELS = {
    "trend_strength": "Tendência forte",
    "risk_adjusted_quality": "Boa relação retorno/risco",
    "recovery_setup": "Possível recuperação",
    "balanced_setup": "Perfil equilibrado",
    "risk_control": "Risco controlado",
    "price_above_moving_averages": "Preço acima das médias",
    "deep_drawdown": "Queda relevante",
    "watchlist": "Acompanhar",
}


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
    st.subheader("Recomendação do momento")
    st.caption(
        "Leitura rápida dos ativos mais bem posicionados no ranking atual. "
        "O score combina tendência, momentum, risco e drawdown."
    )
    card_columns = st.columns(len(top_five))
    for column, (_, row) in zip(card_columns, top_five.iterrows()):
        with column:
            primary_signal = _translate_value(row["primary_signal"], SIGNAL_LABELS)
            secondary_signal = _translate_value(row["secondary_signal"], SIGNAL_LABELS)
            bucket = _translate_value(row["ranking_bucket"], RANKING_BUCKET_LABELS)
            rank_delta = _rank_delta_text(row["rank_delta_7d"])
            st.markdown(
                f"""
                <div style="background:{_bucket_color(row['ranking_bucket'])};padding:1rem;border-radius:0.5rem;color:white;min-height:230px;">
                    <div style="font-size:0.85rem;opacity:0.82;">{bucket} · posição #{int(row['rank_position'])}</div>
                    <div style="font-size:1.5rem;font-weight:700;">{row['asset']}</div>
                    <div style="margin-top:0.35rem;font-size:1rem;">Score: <strong>{row['score']:.2f}</strong></div>
                    <div style="margin-top:0.7rem;font-size:0.9rem;">{primary_signal}</div>
                    <div style="font-size:0.9rem;">{secondary_signal}</div>
                    <div style="margin-top:0.7rem;font-size:0.9rem;">Retorno em 90 dias: <strong>{row['return_90d']:.2%}</strong></div>
                    <div style="font-size:0.9rem;">Volatilidade em 30 dias: <strong>{row['volatility_30d']:.2%}</strong></div>
                    <div style="font-size:0.9rem;">Drawdown em 252 dias: <strong>{row['drawdown_252d']:.2%}</strong></div>
                    <div style="margin-top:0.55rem;font-size:0.85rem;opacity:0.9;">Ranking em 7 dias: {rank_delta}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _executive_column_config() -> dict[str, object]:
    return {
        "Posição": st.column_config.NumberColumn(
            "Posição",
            help="Lugar do ativo no ranking atual. Quanto menor, melhor.",
            format="%d",
        ),
        "Ativo": st.column_config.TextColumn(
            "Ativo",
            help="Código lógico do ativo acompanhado pelo projeto.",
        ),
        "Score": st.column_config.TextColumn(
            "Score",
            help="Nota comparativa do modelo quantitativo. Quanto maior, melhor.",
        ),
        "Faixa do ranking": st.column_config.TextColumn(
            "Faixa do ranking",
            help="Agrupamento simples da posição: Top 3, Top 5, meio ou fim do ranking.",
        ),
        "Sinal principal": st.column_config.TextColumn(
            "Sinal principal",
            help="Principal motivo quantitativo que ajudou o ativo a aparecer nessa posição.",
        ),
        "Sinal de apoio": st.column_config.TextColumn(
            "Sinal de apoio",
            help="Leitura complementar de risco, tendência ou acompanhamento.",
        ),
        "Retorno 90d": st.column_config.TextColumn(
            "Retorno 90d",
            help="Variação acumulada dos últimos 90 pregões observados.",
        ),
        "Volatilidade 30d": st.column_config.TextColumn(
            "Volatilidade 30d",
            help="Oscilação anualizada calculada com os últimos 30 pregões. Menor tende a indicar mais estabilidade.",
        ),
        "Drawdown 252d": st.column_config.TextColumn(
            "Drawdown 252d",
            help="Distância até o maior preço ajustado da janela de 252 pregões. Valor negativo indica queda desde o topo.",
        ),
        "Momentum": st.column_config.TextColumn(
            "Momentum",
            help="Classificação do retorno recente: forte, neutro ou fraco.",
        ),
        "Risco": st.column_config.TextColumn(
            "Risco",
            help="Classificação da volatilidade recente: baixo, médio ou alto.",
        ),
        "Ranking 7d": st.column_config.TextColumn(
            "Ranking 7d",
            help="Mudança de posição contra o snapshot comparável de pelo menos 7 dias atrás.",
        ),
        "Ranking 30d": st.column_config.TextColumn(
            "Ranking 30d",
            help="Mudança de posição contra o snapshot comparável de pelo menos 30 dias atrás.",
        ),
    }


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
            "ranking_bucket": "Faixa",
        },
    )
    scatter.update_layout(
        height=440,
        margin=dict(l=20, r=20, t=30, b=20),
        legend_title_text="Faixa do ranking",
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
            name="Preço ajustado",
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
        title=f"Preço e tendência de {asset}",
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
            name="Ativos elegíveis",
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
        title=f"Histórico de ranking de {asset}",
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
        "Dashboard para apoiar a decisão de aporte mensal com ranking, tendência, risco e leitura do mercado."
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
    metric_1.metric("Última cotação analisada", latest_date.strftime("%d/%m/%Y"))
    metric_2.metric("Ativos elegíveis", eligible_asset_count)
    metric_3.metric("Melhor ativo atual", top_pick_asset)

    show_ineligible = st.toggle("Mostrar ativos inelegíveis", value=False)
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
    st.caption(
        "Tabela para comparar rapidamente posição, sinais, retorno e risco dos ativos elegíveis no snapshot atual."
    )
    with st.expander("O que significa cada coluna?"):
        st.markdown(
            """
            - **Posição**: ordem do ativo no ranking atual; quanto menor, melhor.
            - **Score**: nota comparativa calculada pela Gold. Ela combina tendência, momentum, retorno ajustado ao risco, volatilidade e drawdown.
            - **Sinal principal** e **Sinal de apoio**: resumo em linguagem simples do que mais pesou na leitura quantitativa.
            - **Retorno 90d**: retorno acumulado na janela recente de 90 observações.
            - **Volatilidade 30d**: medida de oscilação recente. Em geral, menor volatilidade indica mais estabilidade.
            - **Drawdown 252d**: distância em relação ao topo recente de 252 observações; valores negativos indicam queda desde o pico.
            - **Ranking 7d/30d**: mostra se o ativo melhorou ou piorou de posição em relação ao histórico comparável.
            """
        )
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
    executive_table["ranking_bucket"] = executive_table["ranking_bucket"].map(
        lambda value: _translate_value(value, RANKING_BUCKET_LABELS)
    )
    executive_table["primary_signal"] = executive_table["primary_signal"].map(
        lambda value: _translate_value(value, SIGNAL_LABELS)
    )
    executive_table["secondary_signal"] = executive_table["secondary_signal"].map(
        lambda value: _translate_value(value, SIGNAL_LABELS)
    )
    executive_table["return_90d"] = _format_pct(executive_table["return_90d"])
    executive_table["volatility_30d"] = _format_pct(executive_table["volatility_30d"])
    executive_table["drawdown_252d"] = _format_pct(executive_table["drawdown_252d"])
    executive_table["momentum_bucket"] = executive_table["momentum_bucket"].map(
        lambda value: _translate_value(value, MOMENTUM_LABELS)
    )
    executive_table["risk_bucket"] = executive_table["risk_bucket"].map(
        lambda value: _translate_value(value, RISK_LABELS)
    )
    executive_table["rank_delta_7d"] = executive_table["rank_delta_7d"].map(_rank_delta_text)
    executive_table["rank_delta_30d"] = executive_table["rank_delta_30d"].map(_rank_delta_text)
    executive_table = executive_table.rename(
        columns={
            "rank_position": "Posição",
            "asset": "Ativo",
            "score": "Score",
            "ranking_bucket": "Faixa do ranking",
            "primary_signal": "Sinal principal",
            "secondary_signal": "Sinal de apoio",
            "return_90d": "Retorno 90d",
            "volatility_30d": "Volatilidade 30d",
            "drawdown_252d": "Drawdown 252d",
            "momentum_bucket": "Momentum",
            "risk_bucket": "Risco",
            "rank_delta_7d": "Ranking 7d",
            "rank_delta_30d": "Ranking 30d",
        }
    )
    st.dataframe(
        executive_table,
        width='stretch',
        hide_index=True,
        column_config=_executive_column_config(),
    )

    st.markdown("### Comparação rápida entre ativos")
    scatter_col, summary_col = st.columns([1.4, 1.0])
    with scatter_col:
        st.plotly_chart(_build_scatter_plot(filtered_snapshot), width='stretch')
    with summary_col:
        summary_table = filtered_snapshot[
            ["rank_position", "asset", "score", "eligibility_status", "ranking_bucket"]
        ].copy()
        summary_table["score"] = _format_number(summary_table["score"])
        summary_table["eligibility_status"] = summary_table["eligibility_status"].map(
            lambda value: _translate_value(value, ELIGIBILITY_LABELS)
        )
        summary_table["ranking_bucket"] = summary_table["ranking_bucket"].map(
            lambda value: _translate_value(value, RANKING_BUCKET_LABELS)
        )
        summary_table = summary_table.rename(
            columns={
                "rank_position": "Posição",
                "asset": "Ativo",
                "score": "Score",
                "eligibility_status": "Elegibilidade",
                "ranking_bucket": "Faixa",
            }
        )
        st.dataframe(summary_table, width='stretch', hide_index=True)

    selected_asset = st.selectbox(
        "Ativo para análise detalhada",
        options=filtered_snapshot["asset"].tolist(),
        index=max(filtered_snapshot["asset"].tolist().index(top_pick_asset), 0)
        if top_pick_asset in filtered_snapshot["asset"].tolist()
        else 0,
    )

    price_history = get_price_history(selected_asset)
    ranking_history = get_ranking_history(selected_asset)
    latest_asset_row = filtered_snapshot[filtered_snapshot["asset"] == selected_asset].iloc[0]

    st.markdown("### Análise detalhada do ativo")
    detail_metric_cols = st.columns(4)
    detail_metric_cols[0].metric("Score atual", f"{latest_asset_row['score']:.2f}")
    detail_metric_cols[1].metric("Posição atual", f"#{int(latest_asset_row['rank_position'])}")
    detail_metric_cols[2].metric("Ranking 7d", _rank_delta_text(latest_asset_row["rank_delta_7d"]))
    detail_metric_cols[3].metric("Ranking 30d", _rank_delta_text(latest_asset_row["rank_delta_30d"]))

    signal_metric_cols = st.columns(4)
    signal_metric_cols[0].metric(
        "Momentum",
        _translate_value(latest_asset_row["momentum_bucket"], MOMENTUM_LABELS),
    )
    signal_metric_cols[1].metric(
        "Risco",
        _translate_value(latest_asset_row["risk_bucket"], RISK_LABELS),
    )
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
        st.plotly_chart(_build_price_chart(price_history, selected_asset), width='stretch')
    with risk_col:
        st.plotly_chart(_build_risk_chart(price_history, selected_asset), width='stretch')

    st.markdown("### Visão de mercado")
    overview_col, history_col = st.columns([1.35, 1.0])
    with overview_col:
        st.plotly_chart(_build_market_chart(market_overview), width='stretch')
    with history_col:
        st.plotly_chart(_build_ranking_history_chart(ranking_history, selected_asset), width='stretch')


if __name__ == "__main__":
    st.set_page_config(
        page_title="Invest Certo Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
    )
    main()

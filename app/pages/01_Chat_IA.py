from __future__ import annotations

import streamlit as st

from app.ai.agent import AIConfigurationError, run_invest_agent
from app.ai.config import load_ai_settings
from app.data_access import GoldDataNotReadyError, ensure_gold_data_ready


def _ensure_chat_history() -> None:
    if "ai_chat_messages" not in st.session_state:
        st.session_state.ai_chat_messages = [
            {
                "role": "assistant",
                "content": (
                    "Oi. Eu sou o chat IA do Invest Certo. Posso analisar os ativos monitorados "
                    "usando os dados históricos da Gold: ranking, retornos, volatilidade, drawdown "
                    "e tendência. Nesta fase inicial eu ainda não consulto internet, notícias ou documentos externos."
                ),
            }
        ]


def _render_setup_status() -> bool:
    settings = load_ai_settings()
    missing_config = []
    if settings.provider != "openai":
        missing_config.append(f"LLM_PROVIDER={settings.provider} ainda não é suportado nesta fase inicial")
    if not settings.openai_api_key:
        missing_config.append("OPENAI_API_KEY não configurada")

    try:
        ensure_gold_data_ready()
    except GoldDataNotReadyError as exc:
        st.warning(str(exc))
        st.code(
            "\n".join(
                [
                    "uv run python pipelines/silver/transform_prices.py",
                    "uv run python pipelines/gold/build_features.py",
                ]
            ),
            language="bash",
        )
        return False

    if missing_config:
        st.warning("Configuração do chat IA pendente.")
        for item in missing_config:
            st.write(f"- {item}")
        st.code(
            "\n".join(
                [
                    "LLM_PROVIDER=openai",
                    "LLM_MODEL=gpt-5-mini",
                    "OPENAI_API_KEY=sua_chave_aqui",
                ]
            ),
            language="dotenv",
        )
        return False

    return True


def main() -> None:
    st.title("Chat IA")
    st.caption(
        "Converse sobre os ativos monitorados usando somente os dados internos da Gold. "
        "A resposta é apoio analítico, não recomendação financeira absoluta."
    )

    is_ready = _render_setup_status()
    _ensure_chat_history()

    for message in st.session_state.ai_chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    prompt = st.chat_input(
        "Pergunte sobre ranking, queda recente, risco, tendência ou comparação entre ativos.",
        disabled=not is_ready,
    )
    if not prompt:
        return

    st.session_state.ai_chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analisando dados internos..."):
            try:
                answer = run_invest_agent(prompt)
            except AIConfigurationError as exc:
                answer = str(exc)
            except Exception as exc:  # pragma: no cover - Streamlit guardrail
                answer = f"Não consegui concluir a análise agora: {exc}"
            st.markdown(answer)

    st.session_state.ai_chat_messages.append({"role": "assistant", "content": answer})


if __name__ == "__main__":
    main()

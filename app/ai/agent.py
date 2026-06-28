from __future__ import annotations

from agno.agent import Agent
from agno.models.openai import OpenAIResponses

from app.ai.config import AISettings, load_ai_settings
from app.ai.prompts import AGENT_INSTRUCTIONS, EXPECTED_OUTPUT
from app.ai.tools import (
    compare_assets,
    detect_recent_drop,
    explain_asset_ranking,
    get_asset_history_summary,
    get_latest_asset_snapshot,
    list_monitored_assets,
)


class AIConfigurationError(RuntimeError):
    """Raised when the conversational agent cannot be configured."""


def create_invest_agent(settings: AISettings | None = None) -> Agent:
    settings = settings or load_ai_settings()
    if settings.provider != "openai":
        raise AIConfigurationError(
            f"Provedor LLM_PROVIDER={settings.provider!r} ainda não é suportado nesta fase inicial."
        )
    if not settings.openai_api_key:
        raise AIConfigurationError(
            "OPENAI_API_KEY não configurada. Defina a chave no .env para usar o chat IA."
        )

    return Agent(
        name="Invest Certo Chat IA",
        model=OpenAIResponses(id=settings.model),
        tools=[
            list_monitored_assets,
            get_latest_asset_snapshot,
            get_asset_history_summary,
            detect_recent_drop,
            compare_assets,
            explain_asset_ranking,
        ],
        instructions=AGENT_INSTRUCTIONS,
        expected_output=EXPECTED_OUTPUT,
        markdown=True,
        add_datetime_to_context=True,
        timezone_identifier="America/Sao_Paulo",
        telemetry=False,
    )


def run_invest_agent(message: str, settings: AISettings | None = None) -> str:
    agent = create_invest_agent(settings)
    response = agent.run(message)
    content = getattr(response, "content", response)
    return str(content)

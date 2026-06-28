from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class AISettings:
    provider: str
    model: str
    openai_api_key: str | None

    @property
    def is_openai_configured(self) -> bool:
        return self.provider == "openai" and bool(self.openai_api_key)


def load_ai_settings() -> AISettings:
    load_dotenv()
    provider = os.getenv("LLM_PROVIDER", "openai").strip().lower()
    model = os.getenv("LLM_MODEL", "gpt-5-mini").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY")
    return AISettings(
        provider=provider,
        model=model,
        openai_api_key=openai_api_key.strip() if openai_api_key else None,
    )


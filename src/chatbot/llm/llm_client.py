from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
from chatbot.config.settings import settings

class LLMClient:
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        raise NotImplementedError

class OpenAIClient(LLMClient):
    def __init__(self):
        from openai import OpenAI
        kwargs: Dict[str, Any] = {}
        if settings.openai_org:
            kwargs["organization"] = settings.openai_org
        if settings.openai_project:
            kwargs["project"] = settings.openai_project
        self.client = OpenAI(api_key=settings.openai_api_key, **kwargs)

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        if not settings.openai_api_key:
            raise RuntimeError("Missing OPENAI_API_KEY in .env")
        resp = self.client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content or ""

class DummyClient(LLMClient):
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        # For offline testing without API key: just echo.
        return "(Dummy mode) Không có OPENAI_API_KEY nên bot không gọi LLM.\n[SOURCE 1]"

def get_llm_client() -> LLMClient:
    # You can expand this to support local LLM later.
    if settings.openai_api_key:
        return OpenAIClient()
    return DummyClient()

def load_prompt(name: str) -> str:
    p = Path("src/chatbot/llm/prompts") / name
    return p.read_text(encoding="utf-8")

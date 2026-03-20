from __future__ import annotations

from datetime import datetime
import json
from time import perf_counter

from openai import OpenAI

from sql_ai_copilot.config.settings import AppSettings
from sql_ai_copilot.logging_utils import get_logger


class OpenAICompatibleClient:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.logger = get_logger("llm")

    def generate(self, provider_name: str, model_name: str, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, object]]:
        provider = self.settings.get_provider(provider_name)
        api_key = self.settings.get_api_key(provider_name)
        payload = {
            "model": model_name or provider.default_model,
            "temperature": 0,
            "max_tokens": 1200,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        started_perf = perf_counter()
        self.logger.info("llm_request %s", json.dumps({"provider": provider_name, "model": payload["model"], "request_payload": payload}, ensure_ascii=False))
        client = OpenAI(api_key=api_key, base_url=provider.base_url, timeout=45.0, max_retries=1)
        response = client.chat.completions.create(**payload)
        ended_at = datetime.now().astimezone().isoformat(timespec="seconds")
        elapsed_ms = round((perf_counter() - started_perf) * 1000)
        trace = {
            "provider": provider_name,
            "model": payload["model"],
            "base_url": provider.base_url,
            "started_at": started_at,
            "ended_at": ended_at,
            "elapsed_ms": elapsed_ms,
            "request_payload": payload,
            "response_id": getattr(response, "id", None),
            "response_model": getattr(response, "model", payload["model"]),
            "response_text": response.choices[0].message.content or "",
        }
        self.logger.info("llm_response %s", json.dumps(trace, ensure_ascii=False))
        return trace["response_text"], trace

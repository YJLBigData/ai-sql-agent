from __future__ import annotations

import json
from datetime import datetime
from time import perf_counter
from urllib import error, request

from sql_ai_copilot.logging_utils import get_logger


class OllamaClient:
    def __init__(self, base_url: str, default_model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        self.logger = get_logger("ollama")

    def list_models(self) -> list[str]:
        try:
            payload = self._post_json("/api/tags", {})
        except Exception as exc:
            self.logger.warning("ollama_list_models_failed error=%s", exc)
            return [self.default_model]
        models = [item.get("name") for item in payload.get("models", []) if item.get("name")]
        return models or [self.default_model]

    def chat(self, model_name: str, system_prompt: str, user_prompt: str) -> tuple[str, dict[str, object]]:
        return self._generate(model_name, system_prompt, user_prompt, stage="chat")

    def rewrite(self, model_name: str, question: str, extra_context: str = "") -> tuple[str, dict[str, object]]:
        system_prompt = "你是本地改写助手。请把问题改写成更清晰、更结构化的业务分析问题，只输出改写后的文本。"
        user_prompt = question if not extra_context else f"{question}\n\n补充上下文:\n{extra_context}"
        return self._generate(model_name, system_prompt, user_prompt, stage="rewrite")

    def classify(self, model_name: str, question: str, labels: list[str]) -> tuple[str, dict[str, object]]:
        system_prompt = "你是本地分类助手。请只输出最匹配的分类标签，不要输出解释。"
        user_prompt = f"问题:\n{question}\n\n可选标签:\n" + "\n".join(f"- {label}" for label in labels)
        return self._generate(model_name, system_prompt, user_prompt, stage="classify")

    def clarify(self, model_name: str, question: str, context: str) -> tuple[str, dict[str, object]]:
        system_prompt = "你是本地澄清助手。请给出最短、最直接的澄清问题。"
        user_prompt = f"原问题:\n{question}\n\n上下文:\n{context}"
        return self._generate(model_name, system_prompt, user_prompt, stage="clarify")

    def _generate(self, model_name: str, system_prompt: str, user_prompt: str, stage: str) -> tuple[str, dict[str, object]]:
        payload = {
            "model": model_name or self.default_model,
            "stream": False,
            "options": {"temperature": 0},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        started_perf = perf_counter()
        self.logger.info(
            "ollama_request %s",
            json.dumps({"stage": stage, "base_url": self.base_url, "request_payload": payload}, ensure_ascii=False),
        )
        response = self._post_json("/api/chat", payload)
        elapsed_ms = round((perf_counter() - started_perf) * 1000)
        trace = {
            "provider": "local",
            "model": payload["model"],
            "base_url": self.base_url,
            "started_at": started_at,
            "ended_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "elapsed_ms": elapsed_ms,
            "request_payload": payload,
            "response_id": response.get("created_at"),
            "response_model": response.get("model", payload["model"]),
            "response_text": ((response.get("message") or {}).get("content") or "").strip(),
            "usage": {
                "prompt_tokens": response.get("prompt_eval_count"),
                "completion_tokens": response.get("eval_count"),
                "total_tokens": (response.get("prompt_eval_count") or 0) + (response.get("eval_count") or 0),
            },
        }
        self.logger.info("ollama_response %s", json.dumps(trace, ensure_ascii=False))
        return trace["response_text"], trace

    def _post_json(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=90) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Ollama 请求失败: {exc.code} {message}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Ollama 服务不可用: {exc}") from exc

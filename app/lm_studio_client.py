from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

LM_STUDIO_BASE_URL: str = os.environ.get("LM_STUDIO_BASE_URL", "http://127.0.0.1:1234/v1")
LM_STUDIO_MODEL: str = os.environ.get("LM_STUDIO_MODEL", "google/gemma-4-e2b")

IMPORTANCE_SCORE_MIN: float = 0.05
IMPORTANCE_SCORE_MAX: float = 0.95


def clamp_importance(value: float) -> float:
    return min(IMPORTANCE_SCORE_MAX, max(IMPORTANCE_SCORE_MIN, float(value)))


def extract_message_content(result: dict[str, Any]) -> str:
    choices = result.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError(
            f"LM Studio nie zwróciło choices. Odpowiedź: {json.dumps(result, ensure_ascii=False)[:1200]}"
        )
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        raise ValueError(
            f"LM Studio nie zwróciło message. Odpowiedź: {json.dumps(result, ensure_ascii=False)[:1200]}"
        )
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    reasoning_content = message.get("reasoning_content")
    if isinstance(reasoning_content, str) and reasoning_content.strip():
        return reasoning_content
    raise ValueError(
        "LM Studio zwróciło pusty content i reasoning_content. "
        f"Odpowiedź: {json.dumps(result, ensure_ascii=False)[:1200]}"
    )


def call_lm_studio(
    messages: list[dict[str, str]],
    response_format: dict[str, Any],
    *,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> str:
    payload = {
        "model": LM_STUDIO_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "response_format": response_format,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{LM_STUDIO_BASE_URL}/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
        return extract_message_content(json.loads(raw))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ConnectionError(
            f"LM Studio zwróciło HTTP {exc.code} dla modelu {LM_STUDIO_MODEL} "
            f"pod {LM_STUDIO_BASE_URL}. Treść: {body[:1200]}"
        ) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Nie można połączyć się z LM Studio pod {LM_STUDIO_BASE_URL}. Szczegóły: {exc}"
        ) from exc
